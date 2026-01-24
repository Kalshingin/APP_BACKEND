"""
VAS Purchase Module - Production Grade
Handles airtime and data purchases with dynamic pricing and emergency recovery

Security: API keys in environment variables, idempotency protection
Providers: Monnify (primary), Peyflex (fallback)
Features: Dynamic pricing, emergency pricing recovery, retention messaging
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from bson import ObjectId
import os
import requests
import uuid
import json
import time
from utils.dynamic_pricing_engine import get_pricing_engine, calculate_vas_price
from utils.emergency_pricing_recovery import tag_emergency_transaction
from blueprints.notifications import create_user_notification
from blueprints.vas_wallet import push_balance_update

def init_vas_purchase_blueprint(mongo, token_required, serialize_doc):
    vas_purchase_bp = Blueprint('vas_purchase', __name__, url_prefix='/api/vas/purchase')
    
    # Environment variables (NEVER hardcode these)
    MONNIFY_API_KEY = os.environ.get('MONNIFY_API_KEY', '')
    MONNIFY_SECRET_KEY = os.environ.get('MONNIFY_SECRET_KEY', '')
    MONNIFY_CONTRACT_CODE = os.environ.get('MONNIFY_CONTRACT_CODE', '')
    MONNIFY_BASE_URL = os.environ.get('MONNIFY_BASE_URL', 'https://sandbox.monnify.com')
    
    # Monnify Bills API specific
    MONNIFY_BILLS_BASE_URL = f"{MONNIFY_BASE_URL}/api/v1/vas/bills-payment"
    
    PEYFLEX_API_TOKEN = os.environ.get('PEYFLEX_API_TOKEN', '')
    PEYFLEX_BASE_URL = os.environ.get('PEYFLEX_BASE_URL', 'https://client.peyflex.com.ng')
    
    VAS_TRANSACTION_FEE = 30.0
    
    # ==================== HELPER FUNCTIONS ====================
    
    def get_monnify_access_token():
        """Get Monnify access token for Bills API"""
        try:
            import base64
            
            # Create basic auth header
            credentials = f"{MONNIFY_API_KEY}:{MONNIFY_SECRET_KEY}"
            encoded_credentials = base64.b64encode(credentials.encode()).decode()
            
            headers = {
                'Authorization': f'Basic {encoded_credentials}',
                'Content-Type': 'application/json'
            }
            
            url = f"{MONNIFY_BASE_URL}/api/v1/auth/login"
            
            response = requests.post(url, headers=headers, timeout=8)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('requestSuccessful'):
                    access_token = data['responseBody']['accessToken']
                    print(f'Monnify access token obtained: {access_token[:20]}...')
                    return access_token
                else:
                    raise Exception(f"Monnify auth failed: {data.get('responseMessage', 'Unknown error')}")
            else:
                raise Exception(f"Monnify auth HTTP error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f'ERROR: Failed to get Monnify access token: {str(e)}')
            raise Exception(f'Monnify authentication failed: {str(e)}')
    
    def call_monnify_bills_api(endpoint, method='GET', data=None, access_token=None):
        """Generic Monnify Bills API caller"""
        try:
            if not access_token:
                access_token = get_monnify_access_token()
            
            headers = {
                'Authorization': f'Bearer {access_token}',
                'Content-Type': 'application/json'
            }
            
            url = f"{MONNIFY_BILLS_BASE_URL}/{endpoint}"
            
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=8)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=8)
            else:
                raise Exception(f"Unsupported HTTP method: {method}")
            
            print(f'INFO: Monnify Bills API {method} {endpoint}: {response.status_code}')
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f'ERROR: Monnify Bills API error: {response.status_code} - {response.text}')
                raise Exception(f'Monnify Bills API error: {response.status_code} - {response.text}')
                
        except Exception as e:
            print(f'ERROR: Monnify Bills API call failed: {str(e)}')
            raise Exception(f'Monnify Bills API failed: {str(e)}')
    
    def generate_retention_description(base_description, savings_message, discount_applied):
        """Generate retention-focused transaction description"""
        try:
            if discount_applied > 0:
                return f"{base_description} (Saved ₦ {discount_applied:.0f})"
            else:
                return base_description
        except Exception as e:
            print(f'WARNING: Error generating retention description: {str(e)}')
            return base_description  # Fallback to base description
    
    def generate_request_id(user_id, transaction_type):
        """Generate unique request ID for idempotency"""
        timestamp = int(datetime.utcnow().timestamp())
        unique_suffix = str(uuid.uuid4())[:8]
        return f'FICORE_{transaction_type}_{user_id}_{timestamp}_{unique_suffix}'
    
    def check_pending_transaction(user_id, transaction_type, amount, phone_number):
        """Check for pending duplicate transactions (idempotency)"""
        cutoff_time = datetime.utcnow() - timedelta(minutes=5)
        
        pending_txn = mongo.db.vas_transactions.find_one({
            'userId': ObjectId(user_id),
            'type': transaction_type,
            'amount': amount,
            'phoneNumber': phone_number,
            'status': 'PENDING',
            'createdAt': {'$gte': cutoff_time}
        })
        
        return pending_txn
    
    def call_monnify_airtime(network, amount, phone_number, request_id):
        """Call Monnify Bills API for airtime purchase"""
        try:
            # Step 1: Get access token
            access_token = get_monnify_access_token()
            
            # Step 2: Map network to Monnify biller code
            network_mapping = {
                'MTN': 'MTN',
                'AIRTEL': 'AIRTEL', 
                'GLO': 'GLO',
                '9MOBILE': '9MOBILE'
            }
            
            monnify_network = network_mapping.get(network.upper())
            if not monnify_network:
                raise Exception(f'Unsupported network for Monnify: {network}')
            
            # Step 3: Find airtime product for this network
            # Get billers for AIRTIME category
            billers_response = call_monnify_bills_api(
                f'billers?category_code=AIRTIME&size=100', 
                'GET', 
                access_token=access_token
            )
            
            target_biller = None
            for biller in billers_response['responseBody']['content']:
                if biller['name'].upper() == monnify_network:
                    target_biller = biller
                    break
            
            if not target_biller:
                raise Exception(f'Monnify biller not found for network: {network}')
            
            # Step 4: Get airtime products for this biller
            products_response = call_monnify_bills_api(
                f'biller-products?biller_code={target_biller["code"]}&size=100',
                'GET',
                access_token=access_token
            )
            
            # Find airtime product (usually has "Top up" or "Airtime" in name)
            airtime_product = None
            for product in products_response['responseBody']['content']:
                if 'airtime' in product['name'].lower() or 'top up' in product['name'].lower():
                    airtime_product = product
                    break
            
            if not airtime_product:
                raise Exception(f'Monnify airtime product not found for {network}')
            
            print(f'INFO: Using Monnify product: {airtime_product["name"]} (Code: {airtime_product["code"]})')
            
            # Step 5: Validate customer (phone number)
            validation_data = {
                'productCode': airtime_product['code'],
                'customerId': phone_number
            }
            
            validation_response = call_monnify_bills_api(
                'validate-customer',
                'POST',
                validation_data,
                access_token=access_token
            )
            
            print(f'SUCCESS: Monnify customer validation successful for {phone_number}')
            
            # Step 6: Prepare vend request (EXACT match to Monnify API spec)
            vend_data = {
                'productCode': airtime_product['code'],
                'customerId': phone_number,
                'amount': int(amount),
                'emailAddress': 'customer@ficoreafrica.com'  # Default email
            }
            
            # Check if validation reference is required
            vend_instruction = validation_response['responseBody'].get('vendInstruction', {})
            if vend_instruction.get('requireValidationRef', False):
                validation_ref = validation_response['responseBody'].get('validationReference')
                if validation_ref:
                    vend_data['validationReference'] = validation_ref
                    print(f'INFO: Using validation reference: {validation_ref}')
            
            print(f'INFO: Monnify vend payload: {vend_data}')
            
            # Step 7: Execute vend (purchase)
            vend_response = call_monnify_bills_api(
                'vend',
                'POST', 
                vend_data,
                access_token=access_token
            )
            
            vend_result = vend_response['responseBody']
            
            if vend_result.get('vendStatus') == 'SUCCESS':
                print(f'SUCCESS: Monnify airtime purchase successful: {vend_result["transactionReference"]}')
                return {
                    'success': True,
                    'transactionReference': vend_result['transactionReference'],
                    'vendReference': vend_result['vendReference'],
                    'description': vend_result.get('description', 'Airtime purchase successful'),
                    'provider': 'monnify',
                    'vendAmount': vend_result.get('vendAmount', amount),
                    'payableAmount': vend_result.get('payableAmount', amount),
                    'commission': vend_result.get('commission', 0)
                }
            elif vend_result.get('vendStatus') == 'IN_PROGRESS':
                # Poll for status
                print(f'INFO: Monnify transaction in progress, checking status...')
                import time
                time.sleep(3)  # Wait 3 seconds
                
                requery_response = call_monnify_bills_api(
                    f'requery?reference={request_id}',
                    'GET',
                    access_token=access_token
                )
                
                final_result = requery_response['responseBody']
                if final_result.get('vendStatus') == 'SUCCESS':
                    print(f'SUCCESS: Monnify airtime purchase completed: {final_result["transactionReference"]}')
                    return {
                        'success': True,
                        'transactionReference': final_result['transactionReference'],
                        'vendReference': final_result['vendReference'],
                        'description': final_result.get('description', 'Airtime purchase successful'),
                        'provider': 'monnify',
                        'vendAmount': final_result.get('vendAmount', amount),
                        'payableAmount': final_result.get('payableAmount', amount),
                        'commission': final_result.get('commission', 0)
                    }
                else:
                    raise Exception(f'Monnify transaction failed: {final_result.get("description", "Unknown error")}')
            else:
                raise Exception(f'Monnify vend failed: {vend_result.get("description", "Unknown error")}')
                
        except Exception as e:
            print(f'ERROR: Monnify airtime purchase failed: {str(e)}')
            raise Exception(f'Monnify airtime failed: {str(e)}')
    
    def call_monnify_data(network, data_plan_code, phone_number, request_id):
        """Call Monnify Bills API for data purchase"""
        try:
            # Step 1: Get access token
            access_token = get_monnify_access_token()
            
            # Step 2: Map network to Monnify biller code
            network_mapping = {
                'MTN': 'MTN',
                'AIRTEL': 'AIRTEL',
                'GLO': 'GLO', 
                '9MOBILE': '9MOBILE'
            }
            
            monnify_network = network_mapping.get(network.upper())
            if not monnify_network:
                raise Exception(f'Unsupported network for Monnify: {network}')
            
            # Step 3: Find data biller for this network
            billers_response = call_monnify_bills_api(
                f'billers?category_code=DATA_BUNDLE&size=100',
                'GET',
                access_token=access_token
            )
            
            target_biller = None
            for biller in billers_response['responseBody']['content']:
                if biller['name'].upper() == monnify_network:
                    target_biller = biller
                    break
            
            if not target_biller:
                raise Exception(f'Monnify data biller not found for network: {network}')
            
            # Step 4: Get data products for this biller
            products_response = call_monnify_bills_api(
                f'biller-products?biller_code={target_biller["code"]}&size=200',
                'GET',
                access_token=access_token
            )
            
            # Find matching data product by plan code or name
            data_product = None
            for product in products_response['responseBody']['content']:
                # Try to match by code first, then by name patterns
                if (product['code'] == data_plan_code or 
                    data_plan_code in product.get('name', '') or
                    any(keyword in product.get('name', '').lower() for keyword in ['data', 'gb', 'mb'])):
                    data_product = product
                    break
            
            if not data_product:
                # Use first available data product as fallback
                data_products = [p for p in products_response['responseBody']['content'] 
                               if 'data' in p.get('name', '').lower()]
                if data_products:
                    data_product = data_products[0]
                    print(f'WARNING: Using fallback data product: {data_product["name"]}')
                else:
                    raise Exception(f'No data products found for {network}')
            
            print(f'INFO: Using Monnify data product: {data_product["name"]} (Code: {data_product["code"]})')
            
            # Step 5: Validate customer
            validation_data = {
                'productCode': data_product['code'],
                'customerId': phone_number
            }
            
            validation_response = call_monnify_bills_api(
                'validate-customer',
                'POST',
                validation_data,
                access_token=access_token
            )
            
            print(f'SUCCESS: Monnify data customer validation successful for {phone_number}')
            
            # Step 6: Prepare vend request
            vend_amount = data_product.get('price', 0)
            if not vend_amount or vend_amount <= 0:
                raise Exception(f'Invalid data product price: {vend_amount}')
            
            vend_data = {
                'productCode': data_product['code'],
                'customerId': phone_number,
                'amount': vend_amount,
                'emailAddress': 'customer@ficoreafrica.com'
            }
            
            # Check validation reference requirement
            vend_instruction = validation_response['responseBody'].get('vendInstruction', {})
            if vend_instruction.get('requireValidationRef', False):
                validation_ref = validation_response['responseBody'].get('validationReference')
                if validation_ref:
                    vend_data['validationReference'] = validation_ref
                    print(f'INFO: Using validation reference for data: {validation_ref}')
            
            print(f'INFO: Monnify data vend payload: {vend_data}')
            
            # Step 7: Execute vend
            vend_response = call_monnify_bills_api(
                'vend',
                'POST',
                vend_data,
                access_token=access_token
            )
            
            vend_result = vend_response['responseBody']
            
            if vend_result.get('vendStatus') == 'SUCCESS':
                print(f'SUCCESS: Monnify data purchase successful: {vend_result["transactionReference"]}')
                return {
                    'success': True,
                    'transactionReference': vend_result['transactionReference'],
                    'vendReference': vend_result['vendReference'],
                    'description': vend_result.get('description', 'Data purchase successful'),
                    'provider': 'monnify',
                    'vendAmount': vend_result.get('vendAmount', vend_amount),
                    'payableAmount': vend_result.get('payableAmount', vend_amount),
                    'commission': vend_result.get('commission', 0),
                    'productName': data_product['name']
                }
            elif vend_result.get('vendStatus') == 'IN_PROGRESS':
                # Poll for status
                print(f'INFO: Monnify data transaction in progress, checking status...')
                import time
                time.sleep(3)
                
                requery_response = call_monnify_bills_api(
                    f'requery?reference={request_id}',
                    'GET',
                    access_token=access_token
                )
                
                final_result = requery_response['responseBody']
                if final_result.get('vendStatus') == 'SUCCESS':
                    print(f'SUCCESS: Monnify data purchase completed: {final_result["transactionReference"]}')
                    return {
                        'success': True,
                        'transactionReference': final_result['transactionReference'],
                        'vendReference': final_result['vendReference'],
                        'description': final_result.get('description', 'Data purchase successful'),
                        'provider': 'monnify',
                        'vendAmount': final_result.get('vendAmount', vend_amount),
                        'payableAmount': final_result.get('payableAmount', vend_amount),
                        'commission': final_result.get('commission', 0),
                        'productName': data_product['name']
                    }
                else:
                    raise Exception(f'Monnify data transaction failed: {final_result.get("description", "Unknown error")}')
            else:
                raise Exception(f'Monnify data vend failed: {vend_result.get("description", "Unknown error")}')
                
        except Exception as e:
            print(f'ERROR: Monnify data purchase failed: {str(e)}')
            raise Exception(f'Monnify data failed: {str(e)}')

    # ==================== PEYFLEX API FUNCTIONS (FALLBACK) ====================
    
    def call_peyflex_airtime(network, amount, phone_number, request_id):
        """Call Peyflex Airtime API with exact format from documentation"""
        # Use the exact format from Peyflex documentation
        payload = {
            'network': network.lower(),  # Documentation shows lowercase: "mtn"
            'amount': int(amount),
            'mobile_number': phone_number
            # NOTE: Do NOT send request_id - not shown in documentation example
        }
        
        print(f'INFO: Peyflex airtime purchase payload: {payload}')
        print(f'INFO: Using API token: {PEYFLEX_API_TOKEN[:10]}...{PEYFLEX_API_TOKEN[-4:]}')
        
        headers = {
            'Authorization': f'Token {PEYFLEX_API_TOKEN}',  # Documentation shows "Token" not "Bearer"
            'Content-Type': 'application/json',
            'User-Agent': 'FiCore-Backend/1.0'
        }
        
        url = f'{PEYFLEX_BASE_URL}/api/airtime/topup/'
        print(f'INFO: Calling Peyflex airtime API: {url}')
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=12
            )
            
            print(f'INFO: Peyflex airtime response: {response.status_code}')
            print(f'INFO: Response body: {response.text[:500]}')
            
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as json_error:
                    print(f'ERROR: Error parsing Peyflex airtime response: {json_error}')
                    raise Exception(f'Invalid response format from Peyflex: {json_error}')
            elif response.status_code == 400:
                print('WARNING: Peyflex airtime API returned 400 Bad Request')
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', response.text)
                except:
                    error_msg = response.text
                raise Exception(f'Invalid airtime request: {error_msg}')
            elif response.status_code == 403:
                print('WARNING: Peyflex airtime API returned 403 Forbidden')
                print('INFO: This usually means: API token invalid, account not activated, or IP not whitelisted')
                raise Exception('Airtime service access denied - check API credentials and account status')
            elif response.status_code == 404:
                print('WARNING: Peyflex airtime API returned 404 Not Found')
                raise Exception('Airtime endpoint not found - check API URL')
            else:
                print(f'WARNING: Peyflex airtime API error: {response.status_code} - {response.text}')
                raise Exception(f'Peyflex airtime API error: {response.status_code} - {response.text}')
                
        except requests.exceptions.ConnectionError as e:
            print(f'ERROR: Connection error to Peyflex: {str(e)}')
            raise Exception('Unable to connect to Peyflex servers - check network connectivity')
        except requests.exceptions.Timeout as e:
            print(f'ERROR: Timeout error to Peyflex: {str(e)}')
            raise Exception('Peyflex API request timed out - try again later')
        except Exception as e:
            if 'Invalid response format' in str(e) or 'Invalid airtime request' in str(e) or 'access denied' in str(e):
                raise  # Re-raise our custom exceptions
            print(f'ERROR: Unexpected error calling Peyflex: {str(e)}')
            raise Exception(f'Unexpected error with Peyflex API: {str(e)}')
    
    def call_peyflex_data(network, data_plan_code, phone_number, request_id):
        """Call Peyflex Data Purchase API with exact format from documentation"""
        # Use the exact format from Peyflex documentation
        payload = {
            'network': network.lower(),  # Documentation shows lowercase network names
            'plan_code': data_plan_code,  # Use plan_code as shown in docs
            'mobile_number': phone_number
            # NOTE: Do NOT send request_id - not shown in documentation example
        }
        
        print(f'INFO: Peyflex data purchase payload: {payload}')
        print(f'INFO: Using API token: {PEYFLEX_API_TOKEN[:10]}...{PEYFLEX_API_TOKEN[-4:]}')
        
        headers = {
            'Authorization': f'Token {PEYFLEX_API_TOKEN}',  # Documentation shows "Token" not "Bearer"
            'Content-Type': 'application/json',
            'User-Agent': 'FiCore-Backend/1.0'
        }
        
        url = f'{PEYFLEX_BASE_URL}/api/data/purchase/'
        print(f'INFO: Calling Peyflex data purchase API: {url}')
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=12
            )
            
            print(f'INFO: Peyflex data purchase response: {response.status_code}')
            print(f'INFO: Response body: {response.text[:500]}')
            
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as json_error:
                    print(f'ERROR: Error parsing Peyflex data purchase response: {json_error}')
                    raise Exception(f'Invalid response format from Peyflex: {json_error}')
            elif response.status_code == 400:
                print('WARNING: Peyflex data purchase API returned 400 Bad Request')
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', response.text)
                except:
                    error_msg = response.text
                raise Exception(f'Invalid data purchase request: {error_msg}')
            elif response.status_code == 403:
                print('WARNING: Peyflex data purchase API returned 403 Forbidden')
                print('INFO: This usually means: API token invalid, account not activated, or IP not whitelisted')
                raise Exception('Data purchase service access denied - check API credentials and account status')
            elif response.status_code == 404:
                print('WARNING: Peyflex data purchase API returned 404 Not Found')
                raise Exception('Data purchase endpoint not found - check API URL')
            else:
                print(f'WARNING: Peyflex data purchase API error: {response.status_code} - {response.text}')
                raise Exception(f'Peyflex data purchase API error: {response.status_code} - {response.text}')
                
        except requests.exceptions.ConnectionError as e:
            print(f'ERROR: Connection error to Peyflex: {str(e)}')
            raise Exception('Unable to connect to Peyflex servers - check network connectivity')
        except requests.exceptions.Timeout as e:
            print(f'ERROR: Timeout error to Peyflex: {str(e)}')
            raise Exception('Peyflex API request timed out - try again later')
        except Exception as e:
            if 'Invalid response format' in str(e) or 'Invalid data purchase request' in str(e) or 'access denied' in str(e):
                raise  # Re-raise our custom exceptions
            print(f'ERROR: Unexpected error calling Peyflex: {str(e)}')
            raise Exception(f'Unexpected error with Peyflex API: {str(e)}')
    
    # ==================== PRICING ENDPOINTS ====================
    
    @vas_purchase_bp.route('/pricing/calculate', methods=['POST'])
    @token_required
    def calculate_pricing(current_user):
        """
        Calculate dynamic pricing for VAS services
        Supports both airtime and data with subscription-based discounts
        """
        try:
            data = request.json
            service_type = data.get('type', '').lower()  # 'airtime' or 'data'
            network = data.get('network', '').upper()
            amount = float(data.get('amount', 0))
            plan_id = data.get('planId')  # Required for data
            
            if service_type not in ['airtime', 'data']:
                return jsonify({
                    'success': False,
                    'message': 'Invalid service type. Must be airtime or data.'
                }), 400
            
            if not network or amount <= 0:
                return jsonify({
                    'success': False,
                    'message': 'Network and amount are required.'
                }), 400
            
            if service_type == 'data' and not plan_id:
                return jsonify({
                    'success': False,
                    'message': 'Plan ID is required for data pricing.'
                }), 400
            
            # Determine user tier
            user_tier = 'basic'
            if current_user.get('subscriptionStatus') == 'active':
                subscription_plan = current_user.get('subscriptionPlan', 'premium')
                user_tier = subscription_plan.lower()
            
            # Calculate pricing using dynamic engine
            pricing_engine = get_pricing_engine(mongo.db)
            pricing_result = pricing_engine.calculate_selling_price(
                service_type=service_type,
                network=network,
                base_amount=amount,
                user_tier=user_tier,
                plan_id=plan_id
            )
            
            # Get competitive analysis
            competitive_analysis = pricing_engine.get_competitive_analysis(
                service_type, network, amount
            )
            
            return jsonify({
                'success': True,
                'data': {
                    'pricing': pricing_result,
                    'competitive': competitive_analysis,
                    'userTier': user_tier,
                    'timestamp': datetime.utcnow().isoformat() + 'Z'
                },
                'message': 'Pricing calculated successfully'
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error calculating pricing: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to calculate pricing',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_purchase_bp.route('/pricing/plans/<network>', methods=['GET'])
    @token_required
    def get_data_plans_with_pricing(current_user, network):
        """
        Get data plans with dynamic pricing for a specific network
        """
        try:
            # Determine user tier
            user_tier = 'basic'
            if current_user.get('subscriptionStatus') == 'active':
                subscription_plan = current_user.get('subscriptionPlan', 'premium')
                user_tier = subscription_plan.lower()
            
            # Get pricing engine
            pricing_engine = get_pricing_engine(mongo.db)
            
            # Get data plans from Peyflex
            data_plans = pricing_engine.get_peyflex_rates('data', network)
            
            # Add dynamic pricing to each plan
            enhanced_plans = []
            for plan_id, plan_data in data_plans.items():
                base_price = plan_data.get('price', 0)
                
                # Calculate pricing for this plan
                pricing_result = pricing_engine.calculate_selling_price(
                    service_type='data',
                    network=network,
                    base_amount=base_price,
                    user_tier=user_tier,
                    plan_id=plan_id
                )
                
                enhanced_plan = {
                    'id': plan_id,
                    'name': plan_data.get('name', ''),
                    'validity': plan_data.get('validity', 30),
                    'originalPrice': base_price,
                    'sellingPrice': pricing_result['selling_price'],
                    'savings': pricing_result['discount_applied'],
                    'savingsMessage': pricing_result['savings_message'],
                    'margin': pricing_result['margin'],
                    'strategy': pricing_result['strategy_used']
                }
                
                enhanced_plans.append(enhanced_plan)
            
            # Sort by price (cheapest first)
            enhanced_plans.sort(key=lambda x: x['sellingPrice'])
            
            return jsonify({
                'success': True,
                'data': {
                    'network': network.upper(),
                    'plans': enhanced_plans,
                    'userTier': user_tier,
                    'totalPlans': len(enhanced_plans)
                },
                'message': 'Data plans with pricing retrieved successfully'
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error getting data plans with pricing: {str(e)}')
            
            # Fallback to original endpoint
            return get_data_plans(network)

    # ==================== EMERGENCY RECOVERY ENDPOINTS ====================
    
    @vas_purchase_bp.route('/emergency-recovery/process', methods=['POST'])
    @token_required
    def process_emergency_recovery(current_user):
        """
        Process emergency pricing recovery (Admin only)
        Run this periodically to compensate users who paid emergency rates
        """
        try:
            # Check if user is admin
            if not current_user.get('isAdmin', False):
                return jsonify({
                    'success': False,
                    'message': 'Admin access required'
                }), 403
            
            data = request.json
            limit = int(data.get('limit', 50))
            
            from utils.emergency_pricing_recovery import process_emergency_recoveries
            
            recovery_results = process_emergency_recoveries(mongo.db, limit)
            
            # Summary statistics
            total_processed = len(recovery_results)
            completed_recoveries = [r for r in recovery_results if r['status'] == 'completed']
            total_compensated = sum(r.get('overage', 0) for r in completed_recoveries)
            
            return jsonify({
                'success': True,
                'data': {
                    'total_processed': total_processed,
                    'completed_recoveries': len(completed_recoveries),
                    'total_compensated': total_compensated,
                    'results': recovery_results
                },
                'message': f'Processed {total_processed} emergency recoveries, compensated ₦ {total_compensated:.2f}'
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error processing emergency recovery: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to process emergency recovery',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_purchase_bp.route('/emergency-recovery/stats', methods=['GET'])
    @token_required
    def get_emergency_recovery_stats(current_user):
        """
        Get emergency recovery statistics (Admin only)
        """
        try:
            # Check if user is admin
            if not current_user.get('isAdmin', False):
                return jsonify({
                    'success': False,
                    'message': 'Admin access required'
                }), 403
            
            days = int(request.args.get('days', 30))
            
            from utils.emergency_pricing_recovery import EmergencyPricingRecovery
            recovery_system = EmergencyPricingRecovery(mongo.db)
            
            stats = recovery_system.get_recovery_stats(days)
            
            return jsonify({
                'success': True,
                'data': stats,
                'message': f'Emergency recovery stats for last {days} days'
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error getting recovery stats: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to get recovery stats',
                'errors': {'general': [str(e)]}
            }), 500
    
    # ==================== NETWORK AND PLANS ENDPOINTS ====================
    
    @vas_purchase_bp.route('/networks/airtime', methods=['GET'])
    @token_required
    def get_airtime_networks(current_user):
        """Get available airtime networks from Monnify Bills API (primary) with Peyflex fallback"""
        try:
            print('INFO: Fetching airtime networks from Monnify Bills API')
            
            # Try Monnify first
            try:
                access_token = get_monnify_access_token()
                billers_response = call_monnify_bills_api(
                    'billers?category_code=AIRTIME&size=100',
                    'GET',
                    access_token=access_token
                )
                
                # Transform Monnify billers to our format
                networks = []
                for biller in billers_response['responseBody']['content']:
                    networks.append({
                        'id': biller['name'].lower().replace(' ', '_'),
                        'name': biller['name'],
                        'code': biller['code'],
                        'source': 'monnify'
                    })
                
                print(f'SUCCESS: Successfully retrieved {len(networks)} airtime networks from Monnify')
                return jsonify({
                    'success': True,
                    'data': networks,
                    'message': 'Airtime networks retrieved from Monnify Bills API',
                    'source': 'monnify_bills'
                }), 200
                
            except Exception as monnify_error:
                print(f'WARNING: Monnify airtime networks failed: {str(monnify_error)}')
                
                # Fallback to Peyflex
                print('INFO: Falling back to Peyflex for airtime networks')
                
                url = f'{PEYFLEX_BASE_URL}/api/airtime/networks/'
                print(f'INFO: Calling Peyflex airtime networks API: {url}')
                
                response = requests.get(url, timeout=10)
                print(f'INFO: Peyflex airtime networks response status: {response.status_code}')
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        print(f'INFO: Peyflex airtime response: {data}')
                        
                        # Handle different response formats
                        networks_list = []
                        if isinstance(data, dict) and 'networks' in data:
                            networks_list = data['networks']
                        elif isinstance(data, list):
                            networks_list = data
                        else:
                            print('WARNING: Unexpected airtime networks response format')
                            raise Exception('Unexpected response format')
                        
                        # Transform to our format
                        transformed_networks = []
                        for network in networks_list:
                            if isinstance(network, dict):
                                transformed_networks.append({
                                    'id': network.get('id', network.get('identifier', network.get('network_id', ''))),
                                    'name': network.get('name', network.get('network_name', '')),
                                    'source': 'peyflex'
                                })
                            elif isinstance(network, str):
                                # Handle simple string format
                                transformed_networks.append({
                                    'id': network.lower(),
                                    'name': network.upper(),
                                    'source': 'peyflex'
                                })
                        
                        print(f'SUCCESS: Successfully transformed {len(transformed_networks)} airtime networks from Peyflex')
                        return jsonify({
                            'success': True,
                            'data': transformed_networks,
                            'message': 'Airtime networks retrieved from Peyflex (fallback)',
                            'source': 'peyflex_fallback'
                        }), 200
                        
                    except Exception as json_error:
                        print(f'ERROR: Error parsing Peyflex airtime networks response: {json_error}')
                        raise Exception(f'Invalid airtime networks response from Peyflex: {json_error}')
                
                else:
                    print(f'WARNING: Peyflex airtime networks API error: {response.status_code} - {response.text}')
                    raise Exception(f'Peyflex airtime networks API returned {response.status_code}')
            
        except Exception as e:
            print(f'ERROR: Error getting airtime networks from both providers: {str(e)}')
            
            # Return fallback airtime networks
            networks = [
                {'id': 'mtn', 'name': 'MTN', 'source': 'fallback'},
                {'id': 'airtel', 'name': 'Airtel', 'source': 'fallback'},
                {'id': 'glo', 'name': 'Glo', 'source': 'fallback'},
                {'id': '9mobile', 'name': '9mobile', 'source': 'fallback'}
            ]
            
            return jsonify({
                'success': True,
                'data': networks,
                'message': 'Emergency fallback airtime networks (both providers unavailable)',
                'emergency': True
            }), 200

    @vas_purchase_bp.route('/networks/data', methods=['GET'])
    @token_required
    def get_data_networks(current_user):
        """Get available data networks from Monnify Bills API (primary) with Peyflex fallback"""
        try:
            print('INFO: Fetching data networks from Monnify Bills API')
            
            # Try Monnify first
            try:
                access_token = get_monnify_access_token()
                billers_response = call_monnify_bills_api(
                    'billers?category_code=DATA_BUNDLE&size=100',
                    'GET',
                    access_token=access_token
                )
                
                # Transform Monnify billers to our format
                networks = []
                for biller in billers_response['responseBody']['content']:
                    networks.append({
                        'id': biller['name'].lower().replace(' ', '_'),
                        'name': biller['name'],
                        'code': biller['code'],
                        'source': 'monnify'
                    })
                
                print(f'SUCCESS: Successfully retrieved {len(networks)} data networks from Monnify')
                return jsonify({
                    'success': True,
                    'data': networks,
                    'message': 'Data networks retrieved from Monnify Bills API',
                    'source': 'monnify_bills'
                }), 200
                
            except Exception as monnify_error:
                print(f'WARNING: Monnify data networks failed: {str(monnify_error)}')
                
                # Fallback to Peyflex
                print('INFO: Falling back to Peyflex for data networks')
                
                headers = {
                    'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'FiCore-Backend/1.0'
                }
                
                url = f'{PEYFLEX_BASE_URL}/api/data/networks/'
                print(f'INFO: Calling Peyflex networks API: {url}')
                
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    print(f'INFO: Peyflex networks response status: {response.status_code}')
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            print(f'INFO: Peyflex response: {data}')
                            
                            # Handle the correct response format from documentation
                            networks_list = []
                            if isinstance(data, dict):
                                if 'networks' in data:
                                    networks_list = data['networks']
                                    print(f'SUCCESS: Found {len(networks_list)} networks in response.networks')
                                elif 'data' in data:
                                    networks_list = data['data']
                                    print(f'SUCCESS: Found {len(networks_list)} networks in response.data')
                                else:
                                    print(f'WARNING: Dict response without networks/data key: {list(data.keys())}')
                                    networks_list = []
                            elif isinstance(data, list):
                                networks_list = data
                                print(f'SUCCESS: Direct array with {len(networks_list)} networks')
                            else:
                                print(f'WARNING: Unexpected response format: {data}')
                                networks_list = []
                            
                            # Transform to our format
                            transformed_networks = []
                            for network in networks_list:
                                if not isinstance(network, dict):
                                    print(f'WARNING: Skipping non-dict network: {network}')
                                    continue
                                    
                                network_data = {
                                    'id': network.get('identifier', network.get('id', network.get('code', ''))),
                                    'name': network.get('name', network.get('label', 'Unknown Network')),
                                    'source': 'peyflex'
                                }
                                
                                # Only add networks with valid data
                                if network_data['id'] and network_data['name']:
                                    transformed_networks.append(network_data)
                                else:
                                    print(f'WARNING: Skipping invalid network: {network}')
                            
                            print(f'SUCCESS: Successfully transformed {len(transformed_networks)} valid networks from Peyflex')
                            
                            if len(transformed_networks) > 0:
                                return jsonify({
                                    'success': True,
                                    'data': transformed_networks,
                                    'message': 'Data networks retrieved from Peyflex (fallback)',
                                    'source': 'peyflex_fallback'
                                }), 200
                            else:
                                print('WARNING: No valid networks found in Peyflex response')
                                # Fall through to emergency fallback
                                
                        except Exception as json_error:
                            print(f'ERROR: Error parsing Peyflex networks response: {json_error}')
                            print(f'INFO: Raw response: {response.text}')
                            # Fall through to emergency fallback
                    
                    elif response.status_code == 403:
                        print('WARNING: Peyflex networks API returned 403 Forbidden')
                        print('INFO: This usually means: API token invalid, account not activated, or IP not whitelisted')
                        # Fall through to emergency fallback
                    
                    else:
                        print(f'WARNING: Peyflex networks API error: {response.status_code} - {response.text}')
                        # Fall through to emergency fallback
                        
                except requests.exceptions.ConnectionError as e:
                    print(f'ERROR: Connection error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
                except requests.exceptions.Timeout as e:
                    print(f'ERROR: Timeout error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
            
        except Exception as e:
            print(f'ERROR: Error getting data networks from both providers: {str(e)}')
        
        # Emergency fallback data networks
        print('INFO: Using emergency fallback data networks')
        fallback_networks = [
            {'id': 'mtn', 'name': 'MTN', 'source': 'fallback'},
            {'id': 'airtel', 'name': 'Airtel', 'source': 'fallback'},
            {'id': 'glo', 'name': 'Glo', 'source': 'fallback'},
            {'id': '9mobile', 'name': '9mobile', 'source': 'fallback'}
        ]
        
        return jsonify({
            'success': True,
            'data': fallback_networks,
            'message': 'Emergency fallback data networks (both providers unavailable)',
            'emergency': True
        }), 200
    
    # ==================== DATA PLANS ENDPOINT ====================
    
    @vas_purchase_bp.route('/data-plans/<network>', methods=['GET'])
    @token_required
    def get_data_plans(current_user, network):
        """Get data plans for a specific network from Monnify Bills API (primary) with Peyflex fallback"""
        try:
            print(f'INFO: Fetching data plans for network: {network}')
            
            # Try Monnify first
            try:
                access_token = get_monnify_access_token()
                
                # Map network to Monnify biller code
                network_mapping = {
                    'mtn': 'MTN',
                    'airtel': 'AIRTEL',
                    'glo': 'GLO',
                    '9mobile': '9MOBILE'
                }
                
                monnify_network = network_mapping.get(network.lower())
                if not monnify_network:
                    raise Exception(f'Network {network} not supported by Monnify')
                
                # Get billers for DATA_BUNDLE category
                billers_response = call_monnify_bills_api(
                    f'billers?category_code=DATA_BUNDLE&size=100',
                    'GET',
                    access_token=access_token
                )
                
                # Find the target biller
                target_biller = None
                for biller in billers_response['responseBody']['content']:
                    if biller['name'].upper() == monnify_network:
                        target_biller = biller
                        break
                
                if not target_biller:
                    raise Exception(f'Monnify biller not found for network: {network}')
                
                # Get data products for this biller
                products_response = call_monnify_bills_api(
                    f'biller-products?biller_code={target_biller["code"]}&size=200',
                    'GET',
                    access_token=access_token
                )
                
                # Transform Monnify products to our format
                plans = []
                for product in products_response['responseBody']['content']:
                    # Filter for data products
                    if 'data' in product.get('name', '').lower() or 'gb' in product.get('name', '').lower() or 'mb' in product.get('name', '').lower():
                        plan = {
                            'id': product['code'],
                            'name': product['name'],
                            'price': product.get('price', 0),
                            'plan_code': product['code'],
                            'source': 'monnify',
                            'priceType': product.get('priceType', 'FIXED'),
                            'minAmount': product.get('minAmount'),
                            'maxAmount': product.get('maxAmount')
                        }
                        
                        # Extract data volume and duration from metadata if available
                        metadata = product.get('metadata', {})
                        if metadata:
                            plan['volume'] = metadata.get('volume', 0)
                            plan['duration'] = metadata.get('duration', 30)
                            plan['durationUnit'] = metadata.get('durationUnit', 'MONTHLY')
                        
                        plans.append(plan)
                
                if plans:
                    print(f'SUCCESS: Successfully retrieved {len(plans)} data plans from Monnify for {network}')
                    return jsonify({
                        'success': True,
                        'data': plans,
                        'message': f'Data plans for {network.upper()} from Monnify Bills API',
                        'source': 'monnify_bills',
                        'network': network
                    }), 200
                else:
                    raise Exception(f'No data plans found for {network} on Monnify')
                
            except Exception as monnify_error:
                print(f'WARNING: Monnify data plans failed for {network}: {str(monnify_error)}')
                
                # Fallback to Peyflex
                print(f'INFO: Falling back to Peyflex for {network} data plans')
                
                # Validate network ID format - Peyflex uses specific network identifiers
                network_lower = network.lower().strip()
                
                # Known working networks based on Peyflex API discovery
                known_networks = {
                    'mtn': 'mtn_gifting_data',  # Map simple names to full IDs
                    'mtn_gifting': 'mtn_gifting_data',
                    'mtn_sme': 'mtn_sme_data',
                    'airtel': 'airtel_data',
                    'glo': 'glo_data',
                    '9mobile': '9mobile_data'
                }
                
                # Use full network ID if available
                if network_lower in known_networks:
                    full_network_id = known_networks[network_lower]
                    print(f'INFO: Mapped {network} to {full_network_id}')
                else:
                    full_network_id = network_lower
                    print(f'INFO: Using network ID as-is: {full_network_id}')
                
                headers = {
                    'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'FiCore-Backend/1.0'
                }
                
                url = f'{PEYFLEX_BASE_URL}/api/data/plans/?network={full_network_id}'
                print(f'INFO: Calling Peyflex plans API: {url}')
                
                try:
                    response = requests.get(url, headers=headers, timeout=10)
                    print(f'INFO: Peyflex plans response status: {response.status_code}')
                    print(f'INFO: Response preview: {response.text[:500]}')
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            print(f'INFO: Peyflex plans response type: {type(data)}')
                            
                            # Handle the correct response format from documentation
                            plans_list = []
                            if isinstance(data, dict):
                                if 'plans' in data:
                                    plans_list = data['plans']
                                    print(f'SUCCESS: Found {len(plans_list)} plans in response.plans')
                                elif 'data' in data:
                                    plans_list = data['data']
                                    print(f'SUCCESS: Found {len(plans_list)} plans in response.data')
                                else:
                                    print(f'WARNING: Dict response without plans/data key: {list(data.keys())}')
                                    # Try to use the dict itself if it looks like a plan
                                    if 'plan_code' in data or 'amount' in data:
                                        plans_list = [data]
                                    else:
                                        plans_list = []
                            elif isinstance(data, list):
                                plans_list = data
                                print(f'SUCCESS: Direct array with {len(plans_list)} plans')
                            else:
                                print(f'WARNING: Unexpected response format: {data}')
                                plans_list = []
                            
                            # Transform to our format
                            transformed_plans = []
                            for plan in plans_list:
                                if not isinstance(plan, dict):
                                    print(f'WARNING: Skipping non-dict plan: {plan}')
                                    continue
                                    
                                transformed_plan = {
                                    'id': plan.get('plan_code', plan.get('id', '')),
                                    'name': plan.get('label', plan.get('name', plan.get('plan_name', 'Unknown Plan'))),
                                    'price': float(plan.get('amount', plan.get('price', 0))),
                                    'validity': plan.get('validity', plan.get('duration', 30)),  # Default 30 days
                                    'plan_code': plan.get('plan_code', plan.get('id', '')),
                                    'source': 'peyflex'
                                }
                                
                                # Only add plans with valid data
                                if transformed_plan['id'] and transformed_plan['price'] > 0:
                                    transformed_plans.append(transformed_plan)
                                else:
                                    print(f'WARNING: Skipping invalid plan: {plan}')
                            
                            print(f'SUCCESS: Successfully transformed {len(transformed_plans)} valid plans from Peyflex')
                            
                            if len(transformed_plans) > 0:
                                return jsonify({
                                    'success': True,
                                    'data': transformed_plans,
                                    'message': f'Data plans for {network.upper()} from Peyflex (fallback)',
                                    'source': 'peyflex_fallback',
                                    'network_id': full_network_id
                                }), 200
                            else:
                                print(f'WARNING: No valid plans found for {full_network_id}')
                                # Fall through to emergency fallback
                                
                        except Exception as json_error:
                            print(f'ERROR: Error parsing Peyflex plans response: {json_error}')
                            print(f'INFO: Raw response: {response.text}')
                            # Fall through to emergency fallback
                    
                    elif response.status_code == 404:
                        print(f'WARNING: Network {full_network_id} not found on Peyflex (404)')
                        # Fall through to emergency fallback
                    
                    elif response.status_code == 403:
                        print(f'WARNING: Peyflex plans API returned 403 Forbidden')
                        print('INFO: This usually means: API token invalid, account not activated, or IP not whitelisted')
                        # Fall through to emergency fallback
                    
                    else:
                        print(f'WARNING: Peyflex plans API error: {response.status_code} - {response.text}')
                        # Fall through to emergency fallback
                        
                except requests.exceptions.ConnectionError as e:
                    print(f'ERROR: Connection error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
                except requests.exceptions.Timeout as e:
                    print(f'ERROR: Timeout error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
                except Exception as e:
                    print(f'ERROR: Unexpected error calling Peyflex: {str(e)}')
                    # Fall through to emergency fallback
            
        except Exception as e:
            print(f'ERROR: Error in get_data_plans: {str(e)}')
        
        # Return emergency fallback plans
        print(f'INFO: Using emergency fallback plans for {network}')
        emergency_plans = _get_fallback_data_plans(network)
        return jsonify({
                'success': True,
                'data': emergency_plans,
                'message': f'Emergency data plans for {network.upper()} (both providers unavailable)',
                'emergency': True
            }), 200

    def _get_fallback_data_plans(network):
        """Get emergency fallback data plans when all providers fail"""
        network_upper = network.upper()
        network_lower = network.lower()
        return [
            {
                'id': f'{network_lower}_1gb',
                'name': f'{network_upper} 1GB - 30 Days',
                'price': 500,
                'validity': 30,
                'plan_code': f'{network_lower}_1gb',
                'source': 'emergency_fallback'
            },
            {
                'id': f'{network_lower}_2gb', 
                'name': f'{network_upper} 2GB - 30 Days',
                'price': 1000,
                'validity': 30,
                'plan_code': f'{network_lower}_2gb',
                'source': 'emergency_fallback'
            }
        ]
    
    # ==================== VAS PURCHASE ENDPOINTS ====================
    
    @vas_purchase_bp.route('/buy-airtime', methods=['POST'])
    @token_required
    def buy_airtime(current_user):
        """Purchase airtime with dynamic pricing and idempotency protection"""
        try:
            data = request.json
            phone_number = data.get('phoneNumber', '').strip()
            network = data.get('network', '').upper()
            amount = float(data.get('amount', 0))
            
            if not phone_number or not network or amount <= 0:
                return jsonify({
                    'success': False,
                    'message': 'Invalid request data',
                    'errors': {'general': ['Phone number, network, and amount are required']}
                }), 400
            
            if amount < 100 or amount > 5000:
                return jsonify({
                    'success': False,
                    'message': 'Amount must be between ₦ 100 and ₦ 5,000'
                }), 400
            
            user_id = str(current_user['_id'])
            
            # Determine user tier for pricing
            user_tier = 'basic'
            if current_user.get('subscriptionStatus') == 'active':
                subscription_plan = current_user.get('subscriptionPlan', 'premium')
                user_tier = subscription_plan.lower()
            
            # Calculate dynamic pricing
            pricing_result = calculate_vas_price(
                mongo.db, 'airtime', network, amount, user_tier, None, user_id
            )
            
            selling_price = pricing_result['selling_price']
            cost_price = pricing_result['cost_price']
            margin = pricing_result['margin']
            savings_message = pricing_result['savings_message']
            
            # EMERGENCY PRICING DETECTION
            emergency_multiplier = 2.0
            normal_expected_cost = amount * 0.99  # Expected normal cost for airtime
            is_emergency_pricing = cost_price >= (normal_expected_cost * emergency_multiplier * 0.8)  # 80% threshold
            
            if is_emergency_pricing:
                print(f"WARNING: EMERGENCY PRICING DETECTED: Cost ₦ {cost_price} vs Expected ₦ {normal_expected_cost}")
                # Will tag after successful transaction
            
            # CRITICAL: Check for pending duplicate transaction (idempotency)
            pending_txn = check_pending_transaction(user_id, 'AIRTIME', selling_price, phone_number)
            if pending_txn:
                print(f'WARNING: Duplicate airtime request blocked for user {user_id}')
                return jsonify({
                    'success': False,
                    'message': 'A similar transaction is already being processed. Please wait.',
                    'errors': {'general': ['Duplicate transaction detected']}
                }), 409
            
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if not wallet:
                return jsonify({
                    'success': False,
                    'message': 'Wallet not found. Please create a wallet first.'
                }), 404
            
            # Use selling price as total amount (no additional fees)
            total_amount = selling_price
            
            if wallet.get('balance', 0.0) < total_amount:
                return jsonify({
                    'success': False,
                    'message': f'Insufficient wallet balance. Required: ₦ {total_amount:.2f}, Available: ₦ {wallet.get("balance", 0.0):.2f}'
                }), 400
            
            # Generate unique request ID
            request_id = generate_request_id(user_id, 'AIRTIME')
            
            # 🔒 ATOMIC TRANSACTION PATTERN: Create FAILED transaction first
            # This prevents stuck PENDING states if backend crashes during processing
            vas_transaction = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'type': 'AIRTIME',
                'network': network,
                'phoneNumber': phone_number,
                'amount': amount,  # Face value amount
                'sellingPrice': selling_price,
                'costPrice': cost_price,
                'margin': margin,
                'userTier': user_tier,
                'pricingStrategy': pricing_result['strategy_used'],
                'savingsMessage': savings_message,
                'totalAmount': total_amount,
                'status': 'FAILED',  # 🔒 Start as FAILED, update to SUCCESS only when complete
                'failureReason': 'Transaction in progress',  # Will be updated if it actually fails
                'provider': None,
                'requestId': request_id,
                'transactionReference': request_id,  # CRITICAL: Add this field for unique index
                'createdAt': datetime.utcnow()
            }
            
            mongo.db.vas_transactions.insert_one(vas_transaction)
            transaction_id = vas_transaction['_id']
            
            success = False
            provider = 'monnify'
            error_message = ''
            api_response = None
            
            try:
                # Try Monnify first (primary provider)
                api_response = call_monnify_airtime(network, amount, phone_number, request_id)
                success = True
                print(f'SUCCESS: Monnify airtime purchase successful: {request_id}')
            except Exception as monnify_error:
                print(f'WARNING: Monnify failed: {str(monnify_error)}')
                error_message = str(monnify_error)
                
                try:
                    # Fallback to Peyflex
                    api_response = call_peyflex_airtime(network, amount, phone_number, request_id)
                    provider = 'peyflex'
                    success = True
                    print(f'SUCCESS: Peyflex airtime purchase successful (fallback): {request_id}')
                except Exception as peyflex_error:
                    print(f'ERROR: Peyflex failed: {str(peyflex_error)}')
                    error_message = f'Both providers failed. Monnify: {monnify_error}, Peyflex: {peyflex_error}'
            
            if not success:
                # Update transaction to FAILED with proper failure reason
                mongo.db.vas_transactions.update_one(
                    {'_id': transaction_id},
                    {'$set': {'status': 'FAILED', 'failureReason': error_message, 'updatedAt': datetime.utcnow()}}
                )
                return jsonify({
                    'success': False,
                    'message': 'Purchase failed',
                    'errors': {'general': [error_message]}
                }), 500
            
            # Deduct selling price from wallet (not face value)
            new_balance = wallet.get('balance', 0.0) - total_amount
            mongo.db.vas_wallets.update_one(
                {'userId': ObjectId(user_id)},
                {'$set': {'balance': new_balance, 'updatedAt': datetime.utcnow()}}
            )
            
            # 🚀 INSTANT BALANCE UPDATE: Push real-time update to frontend
            push_balance_update(user_id, {
                'type': 'balance_update',
                'new_balance': new_balance,
                'transaction_reference': request_id,
                'amount_debited': total_amount,
                'transaction_type': 'AIRTIME_PURCHASE',
                'network': network,
                'phone_number': phone_number[-4:] + '****',
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
            
            # Update transaction to SUCCESS
            update_result = mongo.db.vas_transactions.update_one(
                {'_id': transaction_id},
                {
                    '$set': {
                        'status': 'SUCCESS',
                        'provider': provider,
                        'providerResponse': api_response,
                        'updatedAt': datetime.utcnow()
                    },
                    '$unset': {
                        'failureReason': ""  # 🔒 Clear failure reason on success
                    }
                }
            )
            
            # CRITICAL: Verify transaction was actually updated
            if update_result.modified_count == 0:
                print(f'ERROR: Failed to update transaction {transaction_id} to SUCCESS')
                print(f'       Transaction ID type: {type(transaction_id)}')
                print(f'       Transaction ID value: {transaction_id}')
                
                # Try to find the transaction to debug
                debug_txn = mongo.db.vas_transactions.find_one({'_id': transaction_id})
                if debug_txn:
                    print(f'       Found transaction with status: {debug_txn.get("status")}')
                else:
                    print(f'       Transaction not found in database!')
            else:
                print(f'SUCCESS: Transaction {transaction_id} updated to SUCCESS status')
                
                # Double-check the update worked
                verify_txn = mongo.db.vas_transactions.find_one({'_id': transaction_id})
                if verify_txn and verify_txn.get('status') == 'SUCCESS':
                    print(f'VERIFIED: Transaction {transaction_id} status is SUCCESS')
                else:
                    print(f'WARNING: Transaction {transaction_id} status verification failed')
                    print(f'         Current status: {verify_txn.get("status") if verify_txn else "NOT_FOUND"}')
            
            # Record corporate revenue (margin earned)
            if margin > 0:
                corporate_revenue = {
                    '_id': ObjectId(),
                    'type': 'VAS_MARGIN',
                    'category': 'AIRTIME_MARGIN',
                    'amount': margin,
                    'userId': ObjectId(user_id),
                    'relatedTransaction': str(transaction_id),
                    'description': f'Airtime margin from user {user_id} - {network}',
                    'status': 'RECORDED',
                    'createdAt': datetime.utcnow(),
                    'metadata': {
                        'network': network,
                        'faceValue': amount,
                        'sellingPrice': selling_price,
                        'costPrice': cost_price,
                        'userTier': user_tier,
                        'strategy': pricing_result['strategy_used'],
                        'emergencyPricing': is_emergency_pricing
                    }
                }
                mongo.db.corporate_revenue.insert_one(corporate_revenue)
                print(f'INFO: Corporate revenue recorded: ₦ {margin} from airtime sale to user {user_id}')
            
            # TAG EMERGENCY TRANSACTIONS FOR RECOVERY
            if is_emergency_pricing:
                try:
                    emergency_tag_id = tag_emergency_transaction(
                        mongo.db, str(transaction_id), cost_price, 'airtime', network
                    )
                    print(f'INFO: Emergency transaction tagged for recovery: {emergency_tag_id}')
                    
                    # Create immediate notification about emergency pricing
                    create_user_notification(
                        mongo=mongo.db,
                        user_id=user_id,
                        category='system',
                        title='⚠️ Emergency Pricing Used',
                        body=f'Your {network} airtime purchase used emergency pricing during system maintenance. We\'ll automatically adjust any overcharges within 24 hours.',
                        related_id=str(transaction_id),
                        metadata={
                            'emergency_cost': cost_price,
                            'transaction_id': str(transaction_id),
                            'recovery_expected': True
                        },
                        priority='high'
                    )
                    
                except Exception as e:
                    print(f'WARNING: Failed to tag emergency transaction: {str(e)}')
                    # Don't fail the transaction if tagging fails
            
            # Auto-create expense entry (auto-bookkeeping)
            base_description = f'Airtime - {network} ₦ {amount} for {phone_number[-4:]}****'
            
            # PASSIVE RETENTION ENGINE: Generate retention-focused description
            retention_description = generate_retention_description(
                base_description,
                savings_message,
                pricing_result.get('discount_applied', 0)
            )
            
            expense_entry = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'amount': amount,  # Record actual purchase amount (₦800, not ₦839) - fees eliminated
                'category': 'Utilities',
                'description': retention_description,  # Use retention-enhanced description
                'date': datetime.utcnow(),
                'tags': ['VAS', 'Airtime', network],
                'vasTransactionId': transaction_id,
                'metadata': {
                    'faceValue': amount,
                    'actualCost': amount,  # Actual cost is now the purchase amount (fees eliminated)
                    'userTier': user_tier,
                    'savingsMessage': savings_message,
                    'originalPrice': pricing_result.get('cost_price', 0) + pricing_result.get('margin', 0),
                    'discountApplied': pricing_result.get('discount_applied', 0),
                    'pricingStrategy': pricing_result.get('strategy_used', 'standard'),
                    'freeFeesApplied': pricing_result.get('free_fee_applied', False),
                    'baseDescription': base_description,  # Store original for reference
                    'retentionEnhanced': True,  # Flag to indicate retention messaging applied
                    'feesEliminated': True,  # Flag to indicate VAS purchase fees have been eliminated
                    'sellingPriceForReference': selling_price  # Keep for reference but don't use for expense amount
                },
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            # Import and apply auto-population for proper title/description
            from utils.expense_utils import auto_populate_expense_fields
            expense_entry = auto_populate_expense_fields(expense_entry)
            
            mongo.db.expenses.insert_one(expense_entry)
            
            print(f'SUCCESS: Airtime purchase complete: User {user_id}, Face Value: ₦ {amount}, Charged: ₦ {selling_price}, Margin: ₦ {margin}, Provider: {provider}')
            
            # RETENTION DATA for Frontend Trust Building
            retention_data = {
                'userTier': user_tier,
                'originalPrice': amount,
                'finalPrice': selling_price,
                'totalSaved': amount - selling_price,
                'savingsMessage': savings_message,
                'subscriptionROI': {
                    'tierName': user_tier.title() if user_tier != 'basic' else 'Basic',
                    'annualCost': 25000 if user_tier == 'gold' else (10000 if user_tier == 'premium' else 0),
                    'monthlyProgress': f"You've saved ₦ {amount - selling_price:.0f} this transaction",
                    'loyaltyNudge': f"Your {user_tier.title()} subscription is working!" if user_tier != 'basic' else "Upgrade to Premium to start saving on every purchase!"
                },
                'retentionDescription': retention_description,
                'emergencyPricing': is_emergency_pricing,
                'priceProtectionActive': is_emergency_pricing
            }

            return jsonify({
                'success': True,
                'data': {
                    'transactionId': str(transaction_id),
                    'requestId': request_id,
                    'phoneNumber': phone_number,  # FIX: Include phone number in response
                    'network': network,  # FIX: Include network in response
                    'faceValue': amount,
                    'amountCharged': selling_price,
                    'margin': margin,
                    'newBalance': new_balance,
                    'provider': provider,
                    'userTier': user_tier,
                    'savingsMessage': savings_message,
                    'pricingStrategy': pricing_result['strategy_used'],
                    'expenseRecorded': True,
                    'retentionData': retention_data  # NEW: Frontend trust data
                },
                'message': f'Airtime purchased successfully! {savings_message}' if savings_message else 'Airtime purchased successfully!'
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error buying airtime: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to purchase airtime',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_purchase_bp.route('/buy-data', methods=['POST'])
    @token_required
    def buy_data(current_user):
        """Purchase data with dynamic pricing and idempotency protection"""
        try:
            data = request.json
            phone_number = data.get('phoneNumber', '').strip()
            network = data.get('network', '').upper()
            data_plan_id = data.get('dataPlanId', '')
            data_plan_name = data.get('dataPlanName', '')
            amount = float(data.get('amount', 0))
            
            # CRITICAL: Enhanced logging for plan mismatch debugging
            print(f'🔍 DATA PLAN PURCHASE REQUEST:')
            print(f'   User: {current_user.get("email", "unknown")}')
            print(f'   Phone: {phone_number}')
            print(f'   Network: {network}')
            print(f'   Plan ID: {data_plan_id}')
            print(f'   Plan Name: {data_plan_name}')
            print(f'   Amount: ₦{amount}')
            print(f'   Full Request: {data}')
            
            if not phone_number or not network or not data_plan_id or amount <= 0:
                return jsonify({
                    'success': False,
                    'message': 'Invalid request data',
                    'errors': {'general': ['Phone number, network, data plan, and amount are required']}
                }), 400
            
            user_id = str(current_user['_id'])
            
            # Determine user tier for pricing
            user_tier = 'basic'
            if current_user.get('subscriptionStatus') == 'active':
                subscription_plan = current_user.get('subscriptionPlan', 'premium')
                user_tier = subscription_plan.lower()
            
            # CRITICAL: Data plans should be sold at face value - NO MARGINS
            # Users should pay exactly what they see in the plan selection
            selling_price = amount  # Sell at exactly the displayed price
            cost_price = amount     # Cost is the same as selling price
            margin = 0.0           # No margin for data plans
            savings_message = ''   # No savings message needed
            
            print(f'💰 DATA PRICING (NO MARGIN POLICY):')
            print(f'   Plan Amount: ₦{amount}')
            print(f'   User Pays: ₦{selling_price} (EXACT MATCH)')
            print(f'   No Margin Added: ₦{margin}')
            print(f'   Policy: Sell data at face value')
            
            # CRITICAL: Plan validation to prevent mismatches
            print(f'💰 DATA PRICING (NO MARGIN POLICY):')
            print(f'   Plan Amount: ₦{amount}')
            print(f'   User Pays: ₦{selling_price} (EXACT MATCH)')
            print(f'   No Margin Added: ₦{margin}')
            print(f'   Policy: Sell data at face value')
            
            # CRITICAL: Validate plan exists in provider systems
            plan_validation_result = validate_data_plan_exists(network, data_plan_id, amount)
            if not plan_validation_result['valid']:
                print(f'❌ PLAN VALIDATION FAILED: {plan_validation_result["error"]}')
                return jsonify({
                    'success': False,
                    'message': f'Data plan validation failed: {plan_validation_result["error"]}',
                    'errors': {'general': [f'Plan {data_plan_id} not available for {network}']},
                    'user_message': {
                        'title': '⚠️ Plan Not Available',
                        'message': f'The selected {network} data plan is currently unavailable. Please try a different plan or network.',
                        'type': 'plan_unavailable',
                        'support_message': 'This plan may have been discontinued or is temporarily unavailable.',
                        'retry_after': '5 minutes',
                    }
                }), 400
            
            # EMERGENCY PRICING DETECTION
            emergency_multiplier = 2.0
            normal_expected_cost = amount  # For data, amount is usually the expected cost
            is_emergency_pricing = cost_price >= (normal_expected_cost * emergency_multiplier * 0.8)  # 80% threshold
            
            if is_emergency_pricing:
                print(f"WARNING: EMERGENCY PRICING DETECTED: Cost ₦ {cost_price} vs Expected ₦ {normal_expected_cost}")
                # Will tag after successful transaction
            
            # CRITICAL: Check for pending duplicate transaction (idempotency)
            pending_txn = check_pending_transaction(user_id, 'DATA', selling_price, phone_number)
            if pending_txn:
                print(f'WARNING: Duplicate data request blocked for user {user_id}')
                return jsonify({
                    'success': False,
                    'message': 'A similar transaction is already being processed. Please wait.',
                    'errors': {'general': ['Duplicate transaction detected']}
                }), 409
            
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if not wallet:
                return jsonify({
                    'success': False,
                    'message': 'Wallet not found. Please create a wallet first.'
                }), 404
            
            # Use selling price as total amount
            total_amount = selling_price
            
            if wallet.get('balance', 0.0) < total_amount:
                return jsonify({
                    'success': False,
                    'message': f'Insufficient wallet balance. Required: ₦ {total_amount:.2f}, Available: ₦ {wallet.get("balance", 0.0):.2f}'
                }), 400
            
            # Generate unique request ID
            request_id = generate_request_id(user_id, 'DATA')
            
            # 🔒 ATOMIC TRANSACTION PATTERN: Create FAILED transaction first
            # This prevents stuck PENDING states if backend crashes during processing
            vas_transaction = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'type': 'DATA',
                'network': network,
                'phoneNumber': phone_number,
                'dataPlan': data_plan_name,
                'dataPlanId': data_plan_id,
                'amount': amount,  # Original plan amount
                'sellingPrice': selling_price,
                'costPrice': cost_price,
                'margin': margin,
                'userTier': user_tier,
                'pricingStrategy': 'no_margin_policy',  # Data plans use no margin policy
                'savingsMessage': savings_message,
                'totalAmount': total_amount,
                'status': 'FAILED',  # 🔒 Start as FAILED, update to SUCCESS only when complete
                'failureReason': 'Transaction in progress',  # Will be updated if it actually fails
                'provider': None,
                'requestId': request_id,
                'transactionReference': request_id,  # CRITICAL: Add this field for unique index
                'createdAt': datetime.utcnow()
            }
            
            mongo.db.vas_transactions.insert_one(vas_transaction)
            transaction_id = vas_transaction['_id']
            
            success = False
            provider = 'monnify'
            error_message = ''
            api_response = None
            actual_plan_delivered = None
            
            try:
                # Try Monnify first (primary provider)
                print(f'🔄 ATTEMPTING MONNIFY DATA PURCHASE:')
                print(f'   Network: {network}')
                print(f'   Plan ID: {data_plan_id}')
                print(f'   Phone: {phone_number}')
                
                api_response = call_monnify_data(network, data_plan_id, phone_number, request_id)
                
                # CRITICAL: Validate that delivered plan matches requested plan
                plan_match_result = validate_delivered_plan(api_response, data_plan_id, data_plan_name, amount)
                if not plan_match_result['matches']:
                    print(f'❌ PLAN MISMATCH DETECTED IN MONNIFY RESPONSE:')
                    print(f'   Requested: {data_plan_name} (₦{amount})')
                    print(f'   Delivered: {plan_match_result["delivered_plan"]}')
                    
                    # Log mismatch for investigation
                    log_plan_mismatch(user_id, 'monnify', {
                        'requested_plan_id': data_plan_id,
                        'requested_plan_name': data_plan_name,
                        'requested_amount': amount,
                        'delivered_plan': plan_match_result['delivered_plan'],
                        'api_response': api_response,
                        'transaction_id': str(transaction_id)
                    })
                    
                    raise Exception(f'Plan mismatch: Requested {data_plan_name} but got {plan_match_result["delivered_plan"]}')
                
                actual_plan_delivered = plan_match_result['delivered_plan']
                success = True
                print(f'✅ MONNIFY DATA PURCHASE SUCCESSFUL: {request_id}')
                print(f'   Delivered Plan: {actual_plan_delivered}')
                
            except Exception as monnify_error:
                print(f'⚠️ MONNIFY FAILED: {str(monnify_error)}')
                error_message = str(monnify_error)
                
                try:
                    # Fallback to Peyflex
                    print(f'🔄 ATTEMPTING PEYFLEX DATA PURCHASE (FALLBACK):')
                    print(f'   Network: {network}')
                    print(f'   Plan ID: {data_plan_id}')
                    print(f'   Phone: {phone_number}')
                    
                    api_response = call_peyflex_data(network, data_plan_id, phone_number, request_id)
                    
                    # CRITICAL: Validate Peyflex response as well
                    plan_match_result = validate_delivered_plan(api_response, data_plan_id, data_plan_name, amount)
                    if not plan_match_result['matches']:
                        print(f'❌ PLAN MISMATCH DETECTED IN PEYFLEX RESPONSE:')
                        print(f'   Requested: {data_plan_name} (₦{amount})')
                        print(f'   Delivered: {plan_match_result["delivered_plan"]}')
                        
                        # Log mismatch for investigation
                        log_plan_mismatch(user_id, 'peyflex', {
                            'requested_plan_id': data_plan_id,
                            'requested_plan_name': data_plan_name,
                            'requested_amount': amount,
                            'delivered_plan': plan_match_result['delivered_plan'],
                            'api_response': api_response,
                            'transaction_id': str(transaction_id)
                        })
                        
                        raise Exception(f'Plan mismatch: Requested {data_plan_name} but got {plan_match_result["delivered_plan"]}')
                    
                    actual_plan_delivered = plan_match_result['delivered_plan']
                    provider = 'peyflex'
                    success = True
                    print(f'✅ PEYFLEX DATA PURCHASE SUCCESSFUL (FALLBACK): {request_id}')
                    print(f'   Delivered Plan: {actual_plan_delivered}')
                    
                except Exception as peyflex_error:
                    print(f'❌ PEYFLEX FAILED: {str(peyflex_error)}')
                    error_message = f'Both providers failed. Monnify: {monnify_error}, Peyflex: {peyflex_error}'
            
            if not success:
                # Update transaction to FAILED with proper failure reason
                mongo.db.vas_transactions.update_one(
                    {'_id': transaction_id},
                    {'$set': {'status': 'FAILED', 'failureReason': error_message, 'updatedAt': datetime.utcnow()}}
                )
                return jsonify({
                    'success': False,
                    'message': 'Purchase failed',
                    'errors': {'general': [error_message]}
                }), 500
            
            # Deduct selling price from wallet
            new_balance = wallet.get('balance', 0.0) - total_amount
            mongo.db.vas_wallets.update_one(
                {'userId': ObjectId(user_id)},
                {'$set': {'balance': new_balance, 'updatedAt': datetime.utcnow()}}
            )
            
            # 🚀 INSTANT BALANCE UPDATE: Push real-time update to frontend
            push_balance_update(user_id, {
                'type': 'balance_update',
                'new_balance': new_balance,
                'transaction_reference': request_id,
                'amount_debited': total_amount,
                'transaction_type': 'DATA_PURCHASE',
                'network': network,
                'phone_number': phone_number[-4:] + '****',
                'plan_name': data_plan_name,
                'timestamp': datetime.utcnow().isoformat() + 'Z'
            })
            
            # Update transaction to SUCCESS
            update_result = mongo.db.vas_transactions.update_one(
                {'_id': transaction_id},
                {
                    '$set': {
                        'status': 'SUCCESS',
                        'provider': provider,
                        'providerResponse': api_response,
                        'updatedAt': datetime.utcnow()
                    },
                    '$unset': {
                        'failureReason': ""  # 🔒 Clear failure reason on success
                    }
                }
            )
            
            # CRITICAL: Verify transaction was actually updated
            if update_result.modified_count == 0:
                print(f'ERROR: Failed to update data transaction {transaction_id} to SUCCESS')
                print(f'       Transaction ID type: {type(transaction_id)}')
                print(f'       Transaction ID value: {transaction_id}')
                
                # Try to find the transaction to debug
                debug_txn = mongo.db.vas_transactions.find_one({'_id': transaction_id})
                if debug_txn:
                    print(f'       Found transaction with status: {debug_txn.get("status")}')
                else:
                    print(f'       Transaction not found in database!')
            else:
                print(f'SUCCESS: Data transaction {transaction_id} updated to SUCCESS status')
                
                # Double-check the update worked
                verify_txn = mongo.db.vas_transactions.find_one({'_id': transaction_id})
                if verify_txn and verify_txn.get('status') == 'SUCCESS':
                    print(f'VERIFIED: Data transaction {transaction_id} status is SUCCESS')
                else:
                    print(f'WARNING: Data transaction {transaction_id} status verification failed')
                    print(f'         Current status: {verify_txn.get("status") if verify_txn else "NOT_FOUND"}')
            
            # NO CORPORATE REVENUE RECORDING - Data plans sold at cost with no margin
            
            # TAG EMERGENCY TRANSACTIONS FOR RECOVERY
            if is_emergency_pricing:
                try:
                    emergency_tag_id = tag_emergency_transaction(
                        mongo.db, str(transaction_id), cost_price, 'data', network
                    )
                    print(f'INFO: Emergency transaction tagged for recovery: {emergency_tag_id}')
                    
                    # Create immediate notification about emergency pricing
                    create_user_notification(
                        mongo=mongo.db,
                        user_id=user_id,
                        category='system',
                        title='⚠️ Emergency Pricing Used',
                        body=f'Your {network} {data_plan_name} purchase used emergency pricing during system maintenance. We\'ll automatically adjust any overcharges within 24 hours.',
                        related_id=str(transaction_id),
                        metadata={
                            'emergency_cost': cost_price,
                            'transaction_id': str(transaction_id),
                            'recovery_expected': True,
                            'plan_name': data_plan_name
                        },
                        priority='high'
                    )
                    
                except Exception as e:
                    print(f'WARNING: Failed to tag emergency transaction: {str(e)}')
                    # Don't fail the transaction if tagging fails
            
            # PASSIVE RETENTION ENGINE: Generate retention-focused description
            base_description = f'Data - {network} {data_plan_name} for {phone_number[-4:]}****'
            discount_applied = amount - selling_price  # Calculate actual discount
            retention_description = generate_retention_description(
                base_description,
                savings_message,
                discount_applied
            )
            
            # Auto-create expense entry (auto-bookkeeping) - EXACT AMOUNT ONLY
            expense_entry = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'amount': amount,  # Record EXACT plan amount (no margins added)
                'category': 'Utilities',
                'description': f'Data - {network} {data_plan_name} for {phone_number[-4:]}****',
                'date': datetime.utcnow(),
                'tags': ['VAS', 'Data', network],
                'vasTransactionId': transaction_id,
                'metadata': {
                    'planName': data_plan_name,
                    'planId': data_plan_id,
                    'phoneNumber': phone_number,
                    'network': network,
                    'originalAmount': amount,
                    'actualCost': amount,  # Exact amount paid
                    'userTier': user_tier,
                    'noMarginPolicy': True,  # Flag indicating no margin was added
                    'pricingTransparency': 'User pays exactly what they see in plan selection'
                },
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            # Import and apply auto-population for proper title/description
            from utils.expense_utils import auto_populate_expense_fields
            expense_entry = auto_populate_expense_fields(expense_entry)
            
            mongo.db.expenses.insert_one(expense_entry)
            
            # RETENTION DATA for Frontend Trust Building
            retention_data = {
                'userTier': user_tier,
                'originalPrice': amount,
                'finalPrice': selling_price,
                'totalSaved': discount_applied,
                'savingsMessage': savings_message,
                'subscriptionROI': {
                    'tierName': user_tier.title() if user_tier != 'basic' else 'Basic',
                    'annualCost': 25000 if user_tier == 'gold' else (10000 if user_tier == 'premium' else 0),
                    'monthlyProgress': f"You've saved ₦ {discount_applied:.0f} this transaction",
                    'loyaltyNudge': f"Your {user_tier.title()} subscription is working!" if user_tier != 'basic' else "Upgrade to Premium to start saving on every purchase!"
                },
                'retentionDescription': retention_description,
                'emergencyPricing': is_emergency_pricing,
                'priceProtectionActive': is_emergency_pricing,
                'planDetails': {
                    'network': network,
                    'planName': data_plan_name,
                    'validity': '30 days'  # Could be dynamic based on plan
                }
            }

            print(f'SUCCESS: Data purchase complete: User {user_id}, Plan: {data_plan_name}, Amount: ₦{amount} (NO MARGIN), Provider: {provider}')
            
            return jsonify({
                'success': True,
                'data': {
                    'transactionId': str(transaction_id),
                    'requestId': request_id,
                    'phoneNumber': phone_number,
                    'network': network,
                    'planName': data_plan_name,
                    'planId': data_plan_id,
                    'amount': amount,  # User pays exactly what they see
                    'amountCharged': amount,  # Same as amount - no margin
                    'margin': 0.0,  # No margin for data plans
                    'newBalance': new_balance,
                    'provider': provider,
                    'userTier': user_tier,
                    'pricingPolicy': 'No margin - pay exactly what you see',
                    'expenseRecorded': True,
                    'transparentPricing': True
                },
                'message': f'Data purchased successfully! You paid exactly ₦{amount} as displayed.'
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error buying data: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to purchase data',
                'errors': {'general': [str(e)]}
            }), 500
    
    return vas_purchase_bp

# ==================== PLAN VALIDATION FUNCTIONS ====================

def validate_data_plan_exists(network, plan_id, expected_amount):
    """
    Validate that a data plan exists in provider systems before purchase
    Returns: {'valid': bool, 'error': str, 'plan_details': dict}
    """
    try:
        print(f'🔍 VALIDATING PLAN: {network} - {plan_id} - ₦{expected_amount}')
        
        # Try to fetch current plans from both providers
        monnify_plans = []
        peyflex_plans = []
        
        # Check Monnify first
        try:
            from utils.monnify_utils import get_monnify_access_token, call_monnify_bills_api
            access_token = get_monnify_access_token()
            network_mapping = {
                'MTN': 'MTN',
                'AIRTEL': 'AIRTEL', 
                'GLO': 'GLO',
                '9MOBILE': '9MOBILE'
            }
            
            monnify_network = network_mapping.get(network.upper())
            if monnify_network:
                # Get Monnify plans (simplified version of get_data_plans logic)
                billers_response = call_monnify_bills_api(
                    f'billers?category_code=DATA_BUNDLE&size=100',
                    'GET',
                    access_token=access_token
                )
                
                target_biller = None
                for biller in billers_response['responseBody']['content']:
                    if biller['name'].upper() == monnify_network:
                        target_biller = biller
                        break
                
                if target_biller:
                    products_response = call_monnify_bills_api(
                        f'biller-products?biller_code={target_biller["code"]}&size=200',
                        'GET',
                        access_token=access_token
                    )
                    
                    for product in products_response['responseBody']['content']:
                        if product['code'] == plan_id:
                            monnify_plans.append({
                                'id': product['code'],
                                'name': product['name'],
                                'price': product.get('price', 0),
                                'source': 'monnify'
                            })
                            break
                            
        except Exception as e:
            print(f'⚠️ Monnify plan validation failed: {str(e)}')
        
        # Check Peyflex
        try:
            from config.environment import PEYFLEX_API_TOKEN, PEYFLEX_BASE_URL
            import requests
            
            headers = {
                'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                'Content-Type': 'application/json'
            }
            
            # Map network for Peyflex
            network_mapping = {
                'MTN': 'mtn_gifting_data',
                'AIRTEL': 'airtel_data',
                'GLO': 'glo_data',
                '9MOBILE': '9mobile_data'
            }
            
            peyflex_network = network_mapping.get(network.upper(), network.lower())
            url = f'{PEYFLEX_BASE_URL}/api/data/plans/?network={peyflex_network}'
            
            response = requests.get(url, headers=headers, timeout=15)
            if response.status_code == 200:
                data = response.json()
                plans_list = data.get('plans', data.get('data', []))
                
                for plan in plans_list:
                    if plan.get('plan_code') == plan_id:
                        peyflex_plans.append({
                            'id': plan.get('plan_code'),
                            'name': plan.get('label', plan.get('name')),
                            'price': float(plan.get('amount', plan.get('price', 0))),
                            'source': 'peyflex'
                        })
                        break
                        
        except Exception as e:
            print(f'⚠️ Peyflex plan validation failed: {str(e)}')
        
        # Validate plan exists in at least one provider
        all_plans = monnify_plans + peyflex_plans
        matching_plans = [p for p in all_plans if p['id'] == plan_id]
        
        if not matching_plans:
            return {
                'valid': False,
                'error': f'Plan {plan_id} not found in any provider system',
                'plan_details': None
            }
        
        # Check if any matching plan has the expected amount
        amount_matches = [p for p in matching_plans if abs(p['price'] - expected_amount) < 1.0]
        
        if not amount_matches:
            print(f'⚠️ AMOUNT MISMATCH WARNING:')
            for plan in matching_plans:
                print(f'   Provider {plan["source"]}: ₦{plan["price"]} (expected ₦{expected_amount})')
            
            # Allow with warning - pricing might be dynamic
            return {
                'valid': True,
                'error': None,
                'plan_details': matching_plans[0],
                'warning': f'Plan amount mismatch: expected ₦{expected_amount}, found ₦{matching_plans[0]["price"]}'
            }
        
        return {
            'valid': True,
            'error': None,
            'plan_details': amount_matches[0]
        }
        
    except Exception as e:
        print(f'❌ Plan validation error: {str(e)}')
        return {
            'valid': False,
            'error': f'Validation failed: {str(e)}',
            'plan_details': None
        }

def validate_delivered_plan(api_response, requested_plan_id, requested_plan_name, requested_amount):
    """
    Validate that the API response matches the requested plan
    Returns: {'matches': bool, 'delivered_plan': str, 'details': dict}
    """
    try:
        if not api_response:
            return {
                'matches': False,
                'delivered_plan': 'No response',
                'details': {}
            }
        
        # Extract plan details from API response
        delivered_plan_name = 'Unknown'
        delivered_amount = 0
        
        # Handle Monnify response format
        if isinstance(api_response, dict):
            if 'description' in api_response:
                delivered_plan_name = api_response['description']
            if 'vendAmount' in api_response:
                delivered_amount = float(api_response.get('vendAmount', 0))
            elif 'payableAmount' in api_response:
                delivered_amount = float(api_response.get('payableAmount', 0))
        
        # Handle Peyflex response format
        if 'plan_name' in str(api_response):
            # Extract from Peyflex response
            pass
        
        # Simple validation - check if amounts are close (within ₦50)
        amount_difference = abs(delivered_amount - requested_amount)
        amounts_match = amount_difference <= 50.0
        
        # Check if plan names contain similar keywords
        name_similarity = check_plan_name_similarity(requested_plan_name, delivered_plan_name)
        
        matches = amounts_match and name_similarity
        
        print(f'📊 PLAN VALIDATION RESULT:')
        print(f'   Requested: {requested_plan_name} (₦{requested_amount})')
        print(f'   Delivered: {delivered_plan_name} (₦{delivered_amount})')
        print(f'   Amount Match: {amounts_match} (diff: ₦{amount_difference})')
        print(f'   Name Similarity: {name_similarity}')
        print(f'   Overall Match: {matches}')
        
        return {
            'matches': matches,
            'delivered_plan': f'{delivered_plan_name} (₦{delivered_amount})',
            'details': {
                'delivered_name': delivered_plan_name,
                'delivered_amount': delivered_amount,
                'amount_difference': amount_difference,
                'name_similarity': name_similarity
            }
        }
        
    except Exception as e:
        print(f'❌ Plan validation error: {str(e)}')
        return {
            'matches': False,
            'delivered_plan': f'Validation error: {str(e)}',
            'details': {}
        }

def check_plan_name_similarity(requested_name, delivered_name):
    """
    Check if plan names are similar enough to be considered a match
    """
    try:
        requested_lower = requested_name.lower()
        delivered_lower = delivered_name.lower()
        
        # Extract key terms
        key_terms = ['1gb', '2gb', '500mb', '230mb', 'daily', 'weekly', 'monthly', '7 days', '30 days']
        
        requested_terms = [term for term in key_terms if term in requested_lower]
        delivered_terms = [term for term in key_terms if term in delivered_lower]
        
        # Check for common terms
        common_terms = set(requested_terms) & set(delivered_terms)
        
        # If they share key terms, consider similar
        return len(common_terms) > 0
        
    except Exception:
        return False

def log_plan_mismatch(user_id, provider, mismatch_details):
    """
    Log plan mismatch incidents for investigation and recovery
    """
    try:
        from datetime import datetime
        from bson import ObjectId
        from utils.notification_utils import create_user_notification
        
        mismatch_log = {
            '_id': ObjectId(),
            'userId': ObjectId(user_id),
            'provider': provider,
            'incident_type': 'PLAN_MISMATCH',
            'severity': 'HIGH',
            'details': mismatch_details,
            'status': 'LOGGED',
            'requires_investigation': True,
            'requires_refund': True,
            'created_at': datetime.utcnow(),
            'metadata': {
                'user_impact': 'User received different plan than selected',
                'financial_impact': mismatch_details.get('requested_amount', 0) - mismatch_details.get('delivered_amount', 0),
                'recovery_needed': True
            }
        }
        
        # Store in MongoDB for investigation
        mongo.db.plan_mismatch_logs.insert_one(mismatch_log)
        
        print(f'📝 PLAN MISMATCH LOGGED: {str(mismatch_log["_id"])}')
        print(f'   User: {user_id}')
        print(f'   Provider: {provider}')
        print(f'   Impact: {mismatch_details}')
        
        # Create user notification about the issue
        create_user_notification(
            mongo=mongo.db,
            user_id=user_id,
            category='system',
            title='⚠️ Data Plan Issue Detected',
            body=f'We detected an issue with your recent data purchase. Our team is investigating and will resolve any discrepancies within 24 hours.',
            related_id=mismatch_details.get('transaction_id'),
            metadata={
                'mismatch_log_id': str(mismatch_log['_id']),
                'provider': provider,
                'investigation_required': True,
                'auto_refund_eligible': True
            },
            priority='high'
        )
        
        return str(mismatch_log['_id'])
        
    except Exception as e:
        print(f'❌ Failed to log plan mismatch: {str(e)}')
        return None
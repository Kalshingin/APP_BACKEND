"""
VAS (Value Added Services) Blueprint - Production Grade
Handles wallet management and utility purchases (Airtime, Data, etc.)

Security: API keys in environment variables, idempotency protection, webhook verification
Providers: Monnify (wallet), Peyflex (primary VAS)
Pricing: Dynamic pricing engine with subscription tiers and psychological pricing
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from bson import ObjectId
import os
import requests
import hmac
import hashlib
import uuid
import json
import pymongo
from utils.email_service import get_email_service
from utils.dynamic_pricing_engine import get_pricing_engine, calculate_vas_price
from utils.emergency_pricing_recovery import tag_emergency_transaction
from blueprints.notifications import create_user_notification

def init_vas_blueprint(mongo, token_required, serialize_doc):
    vas_bp = Blueprint('vas', __name__, url_prefix='/api/vas')
    
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
    ACTIVATION_FEE = 100.0
    BVN_VERIFICATION_COST = 10.0
    NIN_VERIFICATION_COST = 60.0
    
    # ==================== MONNIFY BILLS API HELPERS ====================
    
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
            
            response = requests.post(url, headers=headers, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('requestSuccessful'):
                    access_token = data.get('responseBody', {}).get('accessToken')
                    if access_token:
                        return access_token
            
            print(f"Failed to get Monnify token: {response.status_code}")
            return None
            
        except Exception as e:
            print(f"Error getting Monnify token: {e}")
            return None

    def get_monnify_token():
        """Alias for get_monnify_access_token for backward compatibility"""
        return get_monnify_access_token()

    def call_monnify_airtime(network, amount, phone_number, request_id):
        """Call Monnify Bills API for airtime purchase"""
        try:
            token = get_monnify_access_token()
            if not token:
                raise Exception("Failed to get Monnify access token")
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            # Map network codes for Monnify
            network_map = {
                'MTN': 'mtn',
                'GLO': 'glo', 
                'AIRTEL': 'airtel',
                '9MOBILE': '9mobile'
            }
            
            payload = {
                'serviceID': network_map.get(network, network.lower()),
                'amount': amount,
                'phone': phone_number,
                'request_id': request_id
            }
            
            url = f"{MONNIFY_BILLS_BASE_URL}/airtime"
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('requestSuccessful'):
                    return data
            
            raise Exception(f"Monnify airtime failed: {response.text}")
            
        except Exception as e:
            raise Exception(f"Monnify airtime error: {str(e)}")

    def call_monnify_data(network, plan_id, phone_number, request_id):
        """Call Monnify Bills API for data purchase"""
        try:
            token = get_monnify_access_token()
            if not token:
                raise Exception("Failed to get Monnify access token")
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'serviceID': network.lower(),
                'billersCode': phone_number,
                'variation_code': plan_id,
                'request_id': request_id
            }
            
            url = f"{MONNIFY_BILLS_BASE_URL}/data"
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('requestSuccessful'):
                    return data
            
            raise Exception(f"Monnify data failed: {response.text}")
            
        except Exception as e:
            raise Exception(f"Monnify data error: {str(e)}")

    def call_peyflex_airtime(network, amount, phone_number, request_id):
        """Call Peyflex API for airtime purchase (fallback)"""
        try:
            headers = {
                'Authorization': f'Bearer {PEYFLEX_API_TOKEN}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'network': network,
                'amount': amount,
                'phone_number': phone_number,
                'request_id': request_id
            }
            
            url = f"{PEYFLEX_BASE_URL}/api/airtime"
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data
            
            raise Exception(f"Peyflex airtime failed: {response.text}")
            
        except Exception as e:
            raise Exception(f"Peyflex airtime error: {str(e)}")

    def call_peyflex_data(network, plan_id, phone_number, request_id):
        """Call Peyflex API for data purchase (fallback)"""
        try:
            headers = {
                'Authorization': f'Bearer {PEYFLEX_API_TOKEN}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'network': network,
                'plan_id': plan_id,
                'phone_number': phone_number,
                'request_id': request_id
            }
            
            url = f"{PEYFLEX_BASE_URL}/api/data"
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('success'):
                    return data
            
            raise Exception(f"Peyflex data failed: {response.text}")
            
        except Exception as e:
            raise Exception(f"Peyflex data error: {str(e)}")

    # ==================== HELPER FUNCTIONS ====================

    def call_monnify_bills_api(endpoint, method='GET', payload=None):
        """Generic function to call Monnify Bills API"""
        try:
            token = get_monnify_access_token()
            if not token:
                raise Exception("Failed to get Monnify access token")
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            url = f"{MONNIFY_BILLS_BASE_URL}{endpoint}"
            
            if method.upper() == 'GET':
                response = requests.get(url, headers=headers, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=payload, timeout=30)
            else:
                raise Exception(f"Unsupported HTTP method: {method}")
            
            if response.status_code == 200:
                data = response.json()
                if data.get('requestSuccessful'):
                    return data.get('responseBody', {})
            
            raise Exception(f"Monnify Bills API failed: {response.text}")
            
        except Exception as e:
            raise Exception(f"Monnify Bills API error: {str(e)}")

    def check_pending_transaction(user_id, transaction_type, amount, phone_number=None):
        """Check for pending duplicate transactions (idempotency protection)"""
        try:
            # Look for pending transactions in the last 5 minutes
            five_minutes_ago = datetime.utcnow() - timedelta(minutes=5)
            
            query = {
                'userId': ObjectId(user_id),
                'type': transaction_type,
                'status': 'PENDING',
                'createdAt': {'$gte': five_minutes_ago}
            }
            
            # Add amount check (within ₦1 tolerance for floating point)
            query['$and'] = [
                {'sellingPrice': {'$gte': amount - 1}},
                {'sellingPrice': {'$lte': amount + 1}}
            ]
            
            # Add phone number check if provided
            if phone_number:
                query['phoneNumber'] = phone_number
            
            pending_txn = mongo.db.vas_transactions.find_one(query)
            return pending_txn is not None
            
        except Exception as e:
            print(f"Error checking pending transaction: {e}")
            return False

    def generate_request_id(user_id, transaction_type):
        """Generate unique request ID for transactions"""
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        random_suffix = str(uuid.uuid4())[:8]
        return f"{transaction_type}_{user_id[:8]}_{timestamp}_{random_suffix}"

    def generate_retention_description(transaction_type, network, amount, phone_number):
        """Generate user-friendly description for expense entries"""
        if transaction_type == 'AIRTIME':
            return f"{network} Airtime ₦{amount:.0f} to {phone_number}"
        elif transaction_type == 'DATA':
            return f"{network} Data Bundle ₦{amount:.0f} for {phone_number}"
        elif transaction_type == 'BILL':
            return f"Bill Payment ₦{amount:.0f}"
        else:
            return f"{transaction_type} ₦{amount:.0f}"

    def check_eligibility(user_doc):
        """Check if user is eligible for VAS services"""
        try:
            # Basic eligibility checks
            if not user_doc:
                return False, "User not found"
            
            # Check if user has required fields
            if not user_doc.get('email'):
                return False, "Email required"
            
            if not user_doc.get('fullName'):
                return False, "Full name required"
            
            # Check if user is active
            if user_doc.get('status') == 'suspended':
                return False, "Account suspended"
            
            return True, "Eligible"
            
        except Exception as e:
            print(f"Error checking eligibility: {e}")
            return False, "Eligibility check failed"

    def call_monnify_auth(bvn, nin, phone_number, full_name):
        """Call Monnify KYC API for account creation"""
        try:
            token = get_monnify_access_token()
            if not token:
                raise Exception("Failed to get Monnify access token")
            
            headers = {
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            }
            
            payload = {
                'bvn': bvn,
                'nin': nin,
                'phoneNumber': phone_number,
                'fullName': full_name,
                'contractCode': MONNIFY_CONTRACT_CODE
            }
            
            url = f"{MONNIFY_BASE_URL}/api/v1/bank-transfer/reserved-accounts"
            response = requests.post(url, headers=headers, json=payload, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                if data.get('requestSuccessful'):
                    return data.get('responseBody', {})
            
            raise Exception(f"Monnify KYC failed: {response.text}")
            
        except Exception as e:
            raise Exception(f"Monnify KYC error: {str(e)}")

    def get_transaction_display_info(transaction):
        """Get display information for transaction receipts"""
        try:
            txn_type = transaction.get('type', 'UNKNOWN')
            amount = transaction.get('amount', 0)
            metadata = transaction.get('metadata', {})
            
            # Generate description based on transaction type
            if txn_type == 'AIRTIME':
                network = metadata.get('network', transaction.get('network', 'Unknown'))
                phone = metadata.get('phoneNumber', transaction.get('phoneNumber', 'Unknown'))
                description = f'{network} Airtime ₦{amount:,.0f} to {phone}'
                
            elif txn_type == 'DATA':
                network = metadata.get('network', transaction.get('network', 'Unknown'))
                phone = metadata.get('phoneNumber', transaction.get('phoneNumber', 'Unknown'))
                plan_name = metadata.get('planName', 'Data Plan')
                description = f'{network} {plan_name} for {phone}'
                
            elif txn_type == 'BILL':
                provider = metadata.get('provider', 'Bill Payment')
                description = f'{provider} Payment ₦{amount:,.2f}'
                
            elif txn_type == 'WALLET_FUNDING':
                description = f'Wallet Funding ₦{amount:,.2f}'
                
            elif txn_type == 'KYC_VERIFICATION':
                description = 'Account Verification Completed'
                
            else:
                description = f'{txn_type.replace("_", " ").title()} ₦{amount:,.2f}'
            
            # Return description and additional info (for future use)
            additional_info = {
                'formatted_amount': f"₦{amount:,.2f}",
                'transaction_type': txn_type,
                'metadata': metadata
            }
            
            return description, additional_info
            
        except Exception as e:
            print(f"Error getting transaction display info: {e}")
            # Fallback description
            amount = transaction.get('amount', 0)
            txn_type = transaction.get('type', 'Transaction')
            return f'{txn_type} ₦{amount:,.2f}', {}
    
    @vas_bp.route('/pricing/plans/<network>', methods=['GET'])
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
            print(f'❌ Error getting data plans with pricing: {str(e)}')
            
            # Fallback to original endpoint
            return get_data_plans(network)

    # ==================== EMERGENCY RECOVERY ENDPOINTS ====================
    
    @vas_bp.route('/emergency-recovery/process', methods=['POST'])
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
                'message': f'Processed {total_processed} emergency recoveries, compensated ₦{total_compensated:.2f}'
            }), 200
            
        except Exception as e:
            print(f'❌ Error processing emergency recovery: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to process emergency recovery',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/emergency-recovery/stats', methods=['GET'])
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
            print(f'❌ Error getting recovery stats: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to get recovery stats',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/emergency-recovery/trigger', methods=['POST'])
    @token_required
    def trigger_emergency_recovery_job(current_user):
        """
        Manually trigger emergency recovery job (Admin only)
        This is the integration point for the automated recovery script
        """
        try:
            # Check if user is admin
            if not current_user.get('isAdmin', False):
                return jsonify({
                    'success': False,
                    'message': 'Admin access required'
                }), 403
            
            data = request.json or {}
            limit = int(data.get('limit', 50))
            dry_run = data.get('dryRun', False)
            
            print(f'🚨 Manual emergency recovery triggered by admin {current_user.get("email", "unknown")}')
            
            if dry_run:
                # Count pending recoveries for dry run
                pending_count = mongo.db.emergency_pricing_tags.count_documents({
                    'status': 'PENDING_RECOVERY',
                    'recoveryDeadline': {'$gt': datetime.utcnow()}
                })
                
                return jsonify({
                    'success': True,
                    'data': {
                        'dry_run': True,
                        'pending_recoveries': pending_count,
                        'would_process': min(pending_count, limit)
                    },
                    'message': f'Dry run: {pending_count} pending recoveries found'
                }), 200
            
            # Execute actual recovery processing
            from utils.emergency_pricing_recovery import process_emergency_recoveries
            
            recovery_results = process_emergency_recoveries(mongo.db, limit)
            
            # Enhanced response with detailed results
            if recovery_results.get('status') == 'completed':
                results = recovery_results.get('results', [])
                completed_recoveries = [r for r in results if r.get('status') == 'completed']
                total_compensated = sum(r.get('overage', 0) for r in completed_recoveries)
                
                return jsonify({
                    'success': True,
                    'data': {
                        'total_processed': len(results),
                        'completed_recoveries': len(completed_recoveries),
                        'total_compensated': total_compensated,
                        'results': results,
                        'triggered_by': current_user.get('email', 'unknown'),
                        'triggered_at': datetime.utcnow().isoformat() + 'Z'
                    },
                    'message': f'Recovery job completed: {len(completed_recoveries)} recoveries processed, ₦{total_compensated:.2f} compensated'
                }), 200
            
            elif recovery_results.get('status') == 'skipped':
                return jsonify({
                    'success': True,
                    'data': {
                        'skipped': True,
                        'reason': recovery_results.get('reason', 'unknown'),
                        'message': recovery_results.get('message', '')
                    },
                    'message': f'Recovery job skipped: {recovery_results.get("reason", "unknown")}'
                }), 200
            
            else:
                return jsonify({
                    'success': False,
                    'message': f'Recovery job failed: {recovery_results.get("error", "unknown")}',
                    'errors': {'general': [recovery_results.get('error', 'unknown')]}
                }), 500
            
        except Exception as e:
            print(f'❌ Error triggering recovery job: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to trigger recovery job',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/emergency-recovery/schedule', methods=['POST'])
    @token_required
    def schedule_emergency_recovery(current_user):
        """
        Schedule automated emergency recovery job (Admin only)
        This endpoint can be called by external schedulers (cron, etc.)
        """
        try:
            # Check if user is admin or if this is a system call
            api_key = request.headers.get('X-API-Key')
            system_api_key = os.environ.get('SYSTEM_API_KEY', '')
            
            is_admin = current_user.get('isAdmin', False) if current_user else False
            is_system_call = api_key and api_key == system_api_key and system_api_key
            
            if not (is_admin or is_system_call):
                return jsonify({
                    'success': False,
                    'message': 'Admin access or valid API key required'
                }), 403
            
            # Process recoveries automatically
            from utils.emergency_pricing_recovery import process_emergency_recoveries
            
            recovery_results = process_emergency_recoveries(mongo.db, limit=100)
            
            # Log the scheduled job execution
            caller = current_user.get('email', 'unknown') if current_user else 'system_scheduler'
            print(f'🕐 Scheduled emergency recovery executed by: {caller}')
            
            if recovery_results.get('status') == 'completed':
                results = recovery_results.get('results', [])
                completed_recoveries = [r for r in results if r.get('status') == 'completed']
                total_compensated = sum(r.get('overage', 0) for r in completed_recoveries)
                
                # Create admin notification for significant recoveries
                if len(completed_recoveries) > 10 or total_compensated > 5000:
                    create_user_notification(
                        mongo=mongo.db,
                        user_id='admin',  # Special admin notification
                        category='system',
                        title='🚨 Large Emergency Recovery Batch',
                        body=f'Processed {len(completed_recoveries)} recoveries totaling ₦{total_compensated:.2f}',
                        metadata={
                            'recovery_count': len(completed_recoveries),
                            'total_compensated': total_compensated,
                            'triggered_by': caller
                        },
                        priority='high'
                    )
                
                return jsonify({
                    'success': True,
                    'data': {
                        'scheduled_execution': True,
                        'total_processed': len(results),
                        'completed_recoveries': len(completed_recoveries),
                        'total_compensated': total_compensated,
                        'executed_by': caller,
                        'executed_at': datetime.utcnow().isoformat() + 'Z'
                    },
                    'message': f'Scheduled recovery completed: {len(completed_recoveries)} recoveries, ₦{total_compensated:.2f} compensated'
                }), 200
            
            elif recovery_results.get('status') == 'skipped':
                return jsonify({
                    'success': True,
                    'data': {
                        'scheduled_execution': True,
                        'skipped': True,
                        'reason': recovery_results.get('reason', 'unknown')
                    },
                    'message': f'Scheduled recovery skipped: {recovery_results.get("reason", "unknown")}'
                }), 200
            
            else:
                return jsonify({
                    'success': False,
                    'message': f'Scheduled recovery failed: {recovery_results.get("error", "unknown")}',
                    'errors': {'general': [recovery_results.get('error', 'unknown')]}
                }), 500
            
        except Exception as e:
            print(f'❌ Error in scheduled recovery: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to execute scheduled recovery',
                'errors': {'general': [str(e)]}
            }), 500

    
    @vas_bp.route('/networks/airtime', methods=['GET'])
    @token_required
    def get_airtime_networks(current_user):
        """Get available airtime networks from Monnify Bills API (primary) with Peyflex fallback"""
        try:
            print('🔍 Fetching airtime networks from Monnify Bills API')
            
            # Try Monnify first
            try:
                networks_data = call_monnify_bills_api('/billers?category_code=AIRTIME&size=100')
                
                # Transform Monnify billers to our format
                networks = []
                for biller in networks_data.get('content', []):
                    networks.append({
                        'id': biller['name'].lower().replace(' ', '_'),
                        'name': biller['name'],
                        'code': biller['code'],
                        'source': 'monnify'
                    })
                
                print(f'✅ Successfully retrieved {len(networks)} airtime networks from Monnify')
                return jsonify({
                    'success': True,
                    'data': networks,
                    'message': 'Airtime networks retrieved from Monnify Bills API',
                    'source': 'monnify_bills'
                }), 200
                
            except Exception as monnify_error:
                print(f'⚠️ Monnify airtime networks failed: {str(monnify_error)}')
                
                # Fallback to Peyflex
                print('🔄 Falling back to Peyflex for airtime networks')
                
                url = f'{PEYFLEX_BASE_URL}/api/airtime/networks/'
                response = requests.get(url, timeout=30)
                
                if response.status_code == 200:
                    peyflex_data = response.json()
                    if peyflex_data.get('success'):
                        networks = peyflex_data.get('data', [])
                        
                        print(f'✅ Successfully retrieved {len(networks)} airtime networks from Peyflex')
                        return jsonify({
                            'success': True,
                            'data': networks,
                            'message': 'Airtime networks retrieved from Peyflex (fallback)',
                            'source': 'peyflex'
                        }), 200
                
                # If both fail, return default networks
                default_networks = [
                    {'id': 'mtn', 'name': 'MTN', 'code': 'MTN', 'source': 'default'},
                    {'id': 'glo', 'name': 'Glo', 'code': 'GLO', 'source': 'default'},
                    {'id': 'airtel', 'name': 'Airtel', 'code': 'AIRTEL', 'source': 'default'},
                    {'id': '9mobile', 'name': '9mobile', 'code': '9MOBILE', 'source': 'default'}
                ]
                
                return jsonify({
                    'success': True,
                    'data': default_networks,
                    'message': 'Default airtime networks (providers unavailable)',
                    'source': 'default'
                }), 200
                
        except Exception as e:
            print(f'❌ Error fetching airtime networks: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to fetch airtime networks',
                'errors': {'general': [str(e)]}
            }), 500

    @vas_bp.route('/verify-bvn', methods=['POST'])
    @token_required
    def verify_bvn(current_user):
        """
        Create Monnify account using BVN and NIN - FREE for users (business absorbs ₦70 cost)
        Uses original working approach: send BVN directly to Monnify for account creation
        """
        try:
            data = request.json
            bvn = data.get('bvn', '').strip()
            nin = data.get('nin', '').strip()
            phone_number = data.get('phoneNumber', '').strip()  # Only phone needed
            
            print(f"🔍 BVN Account Creation Request - BVN: {bvn}, NIN: {nin}, Phone: {phone_number}")
            
            # Validate
            if len(bvn) != 11 or not bvn.isdigit():
                return jsonify({
                    'success': False,
                    'message': 'Invalid BVN. Must be 11 digits.'
                }), 400
            
            if len(nin) != 11 or not nin.isdigit():
                return jsonify({
                    'success': False,
                    'message': 'Invalid NIN. Must be 11 digits.'
                }), 400
            
            if not phone_number:
                return jsonify({
                    'success': False,
                    'message': 'Phone number is required.'
                }), 400
            
            # Validate phone number format
            if len(phone_number) < 10 or len(phone_number) > 14:
                return jsonify({
                    'success': False,
                    'message': 'Invalid phone number format.'
                }), 400
            
            # Check eligibility first
            user_id = str(current_user['_id'])
            eligible, _ = check_eligibility(user_id)
            if not eligible:
                return jsonify({
                    'success': False,
                    'message': 'Not eligible yet. Complete more transactions to unlock.'
                }), 403
            
            # Check if user already has verified wallet
            existing_wallet = mongo.db.vas_wallets.find_one({
                'userId': ObjectId(user_id),
                'kycStatus': 'verified'
            })
            if existing_wallet:
                return jsonify({
                    'success': False,
                    'message': 'You already have a verified account.'
                }), 400
            
            # Use user profile data for account creation
            user_name = f"{current_user.get('firstName', '')} {current_user.get('lastName', '')}".strip()
            user_email = current_user.get('email', '').strip()
            
            # If no name in profile, use a default
            if not user_name:
                user_name = f"FiCore User {user_id[:8]}"
            
            print(f"🔍 Account creation details - Name: '{user_name}', Phone: '{phone_number}', Email: '{user_email}'")
            
            # Create dedicated account immediately using Monnify account creation (not verification)
            # This is the original working approach - send BVN directly to create account
            print(f"🏦 Creating Monnify reserved account with BVN: {bvn[:3]}***{bvn[-3:]}")
            
            monnify_response = call_monnify_auth(bvn, nin, phone_number, user_name)
            
            # Extract account details from Monnify response
            accounts = monnify_response.get('accounts', [])
            if not accounts:
                raise Exception("No accounts returned from Monnify")
            
            primary_account = accounts[0]  # Use first account as primary
            
            print(f"✅ Monnify account created successfully with {len(accounts)} banks")
            
            # Update user profile with verified information
            profile_update = {
                'phone': phone_number,  # Save phone number to profile
                'bvn': bvn,          # Save BVN (for future reference)
                'nin': nin,          # Save NIN (for future reference)
                'kycStatus': 'verified',  # Mark KYC as completed
                'kycVerifiedAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            # Only update full name if it's more complete than current profile
            current_display_name = current_user.get('displayName', '').strip()
            if len(user_name) > len(current_display_name):
                profile_update['displayName'] = user_name
            
            # Update user profile
            mongo.db.users.update_one(
                {'_id': ObjectId(user_id)},
                {'$set': profile_update}
            )
            
            print(f"✅ Updated user profile with KYC data: phone={phone_number}, KYC=verified")
            
            # Create wallet record with KYC verification
            wallet_data = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'balance': 0.0,
                'accountReference': monnify_response.get('accountReference'),
                'contractCode': monnify_response.get('contractCode'),
                'accounts': accounts,
                'status': 'ACTIVE',
                'tier': 'TIER_2',  # Full KYC verified account
                'kycVerified': True,
                'kycStatus': 'verified',
                'bvn': bvn,
                'nin': nin,
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.vas_wallets.insert_one(wallet_data)
            )
            
            # Record business expense for account creation (business absorbs verification costs)
            business_expense = {
                '_id': ObjectId(),
                'type': 'ACCOUNT_CREATION_COSTS',
                'amount': 70.0,  # ₦10 BVN + ₦60 NIN (absorbed by business)
                'userId': ObjectId(user_id),
                'description': f'Account creation costs for user {user_id} (BVN/NIN verification absorbed by business)',
                'status': 'RECORDED',
                'createdAt': datetime.utcnow(),
                'metadata': {
                    'bvnCost': 10.0,
                    'ninCost': 60.0,
                    'businessExpense': True,
                    'userCharged': False,
                    'accountCreation': True
                }
            }
            mongo.db.business_expenses.insert_one(business_expense)
            
            print(f'SUCCESS: FREE account creation completed for user {user_id}: {user_name}')
            print(f'EXPENSE: Business expense recorded: ₦70 verification costs (absorbed by business)')
            
            # Return all accounts for frontend to choose from
            return jsonify({
                'success': True,
                'data': {
                    'accounts': accounts,  # All available bank accounts
                    'accountReference': monnify_response.get('accountReference'),
                    'contractCode': monnify_response.get('contractCode'),
                    'tier': 'TIER_2',
                    'kycVerified': True,
                    'verifiedName': user_name,
                    'createdAt': wallet_data['createdAt'].isoformat() + 'Z',
                    # Keep backward compatibility - return first account as default
                    'defaultAccount': {
                        'accountNumber': primary_account.get('accountNumber', ''),
                        'accountName': primary_account.get('accountName', ''),
                        'bankName': primary_account.get('bankName', 'Wema Bank'),
                        'bankCode': primary_account.get('bankCode', '035'),
                    }
                },
                'message': f'Account created successfully with {len(accounts)} available banks!'
            }), 201
            
        except Exception as e:
            print(f'ERROR: Error verifying BVN/NIN: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Verification failed',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/verification/validate-details', methods=['POST'])
    @token_required
    def validate_kyc_details(current_user):
        """Pre-validate BVN/NIN format before payment to reduce errors"""
        try:
            data = request.get_json()
            
            bvn = data.get('bvn', '').strip()
            nin = data.get('nin', '').strip()
            
            errors = []
            
            # Validate BVN format
            if not bvn:
                errors.append('BVN is required')
            elif len(bvn) != 11 or not bvn.isdigit():
                errors.append('BVN must be exactly 11 digits')
            
            # Validate NIN format
            if not nin:
                errors.append('NIN is required')
            elif len(nin) != 11 or not nin.isdigit():
                errors.append('NIN must be exactly 11 digits')
            
            # Check if BVN and NIN are the same (common mistake)
            if bvn and nin and bvn == nin:
                errors.append('BVN and NIN cannot be the same number')
            
            if errors:
                return jsonify({
                    'success': False,
                    'message': 'Please correct the following errors before proceeding',
                    'errors': {'validation': errors},
                    'warning': 'Double-check your details to avoid losing the ₦70 non-refundable government verification fee'
                }), 400
            
            return jsonify({
                'success': True,
                'message': 'Details format validated successfully',
                'data': {
                    'bvnValid': True,
                    'ninValid': True,
                    'readyForPayment': True
                },
                'disclaimer': {
                    'nonRefundable': True,
                    'governmentFee': True,
                    'warning': 'IMPORTANT: The ₦70 verification fee is a government charge and is NON-REFUNDABLE. If your BVN/NIN details are incorrect, you will need to pay again.',
                    'advice': 'Please triple-check your BVN and NIN numbers before proceeding to payment.'
                }
            }), 200
            
        except Exception as e:
            print(f'❌ Error validating KYC details: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Validation failed',
                'errors': {'general': [str(e)]}
            }), 500

    # REMOVED: Payment-related endpoints no longer needed
    # BVN/NIN verification is now FREE - business absorbs costs
    # Account creation happens immediately after verification

    @vas_bp.route('/confirm-kyc', methods=['POST'])
    @token_required
    def confirm_kyc(current_user):
        """
        User confirmed the name is correct
        Now create the reserved account with KYC
        """
        try:
            user_id = str(current_user['_id'])
            
            # Get pending verification
            verification = mongo.db.kyc_verifications.find_one({
                'userId': ObjectId(user_id),
                'status': 'pending_confirmation',
                'expiresAt': {'$gt': datetime.utcnow()}
            })
            
            if not verification:
                return jsonify({
                    'success': False,
                    'message': 'Verification expired. Please try again.'
                }), 400
            
            # Check if wallet already exists
            existing_wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if existing_wallet:
                return jsonify({
                    'success': False,
                    'message': 'Wallet already exists.'
                }), 400
            
            # Create reserved account with BVN and NIN (reuse existing wallet creation logic)
            phone_number = verification.get('phoneNumber', current_user.get('phoneNumber', ''))
            user_name = verification['verifiedName']
            monnify_response = call_monnify_auth(verification['bvn'], verification['nin'], phone_number, user_name)
            
            # Extract account details from Monnify response
            accounts = monnify_response.get('accounts', [])
            if not accounts:
                raise Exception("No accounts returned from Monnify")
            
            print(f"✅ Monnify account created successfully with {len(accounts)} banks")
            
            # Create wallet with KYC info (BVN + NIN for full Tier 2)
            wallet = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'balance': 0.0,
                'accountReference': monnify_response.get('accountReference'),
                'accountName': monnify_response.get('accountName'),
                'accounts': accounts,
                'kycStatus': 'verified',
                'kycTier': 2,  # Full Tier 2 compliance with BVN + NIN
                'bvnVerified': True,
                'ninVerified': True,
                'verifiedName': verification['verifiedName'],
                'verificationDate': datetime.utcnow(),
                'isActivated': False,
                'activationFeeDeducted': False,
                'activationDate': None,
                'status': 'active',
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.vas_wallets.insert_one(wallet)
            
            # Update verification status
            mongo.db.kyc_verifications.update_one(
                {'_id': verification['_id']},
                {'$set': {'status': 'confirmed', 'updatedAt': datetime.utcnow()}}
            )
            
            print(f'✅ Reserved account created for user {user_id}')
            
            return jsonify({
                'success': True,
                'data': serialize_doc(wallet),
                'message': 'Account created successfully'
            }), 201
            
        except Exception as e:
            print(f'❌ Error confirming KYC: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to create account',
                'errors': {'general': [str(e)]}
            }), 500
    
    # ==================== WEBHOOK ENDPOINT ====================
    
    @vas_bp.route('/wallet/webhook', methods=['POST'])
    def monnify_webhook():
        """Handle Monnify webhook with HMAC-SHA512 signature verification"""
        
        def process_reserved_account_funding_inline(user_id, amount_paid, transaction_reference, webhook_data):
            """Process reserved account funding inline with idempotent logic"""
            try:
                # CRITICAL: Check if this transaction was already processed (idempotency)
                already_processed = mongo.db.vas_transactions.find_one({"reference": transaction_reference})
                if already_processed:
                    print(f"⚠️ Duplicate transaction ignored: {transaction_reference}")
                    return jsonify({'success': True, 'message': 'Already processed'}), 200
                
                wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
                if not wallet:
                    print(f'❌ Wallet not found for user: {user_id}')
                    return jsonify({'success': False, 'message': 'Wallet not found'}), 404
                
                # Check if user is premium (no deposit fee)
                user = mongo.db.users.find_one({'_id': ObjectId(user_id)})
                is_premium = False
                if user:
                    # CRITICAL FIX: Check multiple premium indicators
                    # 1. Check subscriptionStatus (standard subscription)
                    subscription_status = user.get('subscriptionStatus')
                    if subscription_status == 'active':
                        is_premium = True
                    
                    # 2. Check subscription dates (admin granted or standard)
                    elif user.get('subscriptionStartDate') and user.get('subscriptionEndDate'):
                        subscription_end = user.get('subscriptionEndDate')
                        now = datetime.utcnow()
                        if subscription_end > now:
                            is_premium = True
                            print(f'✅ User {user_id} is premium via subscription dates (ends: {subscription_end})')
                    
                    # 3. Check if user is admin
                    elif user.get('isAdmin', False):
                        is_premium = True
                        print(f'✅ User {user_id} is premium via admin status')
                
                print(f'💰 Premium check for user {user_id}: {is_premium}')
                
                # Apply deposit fee (₦30 for non-premium users)
                deposit_fee = 0.0 if is_premium else VAS_TRANSACTION_FEE
                amount_to_credit = amount_paid - deposit_fee
                
                # Ensure we don't credit negative amounts
                if amount_to_credit <= 0:
                    print(f'⚠️ Amount too small after fee: ₦{amount_paid} - ₦{deposit_fee} = ₦{amount_to_credit}')
                    return jsonify({'success': False, 'message': 'Amount too small to process'}), 400
                
                # SAFETY FIRST: Insert transaction record BEFORE updating wallet balance
                transaction = {
                    '_id': ObjectId(),
                    'userId': ObjectId(user_id),
                    'type': 'WALLET_FUNDING',
                    'amount': amount_to_credit,
                    'amountPaid': amount_paid,
                    'depositFee': deposit_fee,
                    'reference': transaction_reference,
                    'transactionReference': transaction_reference,  # CRITICAL: Add this field for unique index
                    'status': 'SUCCESS',
                    'provider': 'monnify',
                    'metadata': webhook_data,
                    'createdAt': datetime.utcnow()
                }
                
                # Try to insert transaction - if duplicate key error, return success (already processed)
                try:
                    mongo.db.vas_transactions.insert_one(transaction)
                except pymongo.errors.DuplicateKeyError:
                    print(f"⚠️ Duplicate key error - transaction already exists: {transaction_reference}")
                    return jsonify({'success': True, 'message': 'Already processed'}), 200
                
                # ONLY update wallet balance after successful transaction insert
                new_balance = wallet.get('balance', 0.0) + amount_to_credit
                
                mongo.db.vas_wallets.update_one(
                    {'userId': ObjectId(user_id)},
                    {'$set': {'balance': new_balance, 'updatedAt': datetime.utcnow()}}
                )
                
                # Record corporate revenue (₦30 fee)
                if deposit_fee > 0:
                    corporate_revenue = {
                        '_id': ObjectId(),
                        'type': 'SERVICE_FEE',
                        'category': 'DEPOSIT_FEE',
                        'amount': deposit_fee,
                        'userId': ObjectId(user_id),
                        'relatedTransaction': transaction_reference,
                        'description': f'Deposit fee from user {user_id}',
                        'status': 'RECORDED',
                        'createdAt': datetime.utcnow(),
                        'metadata': {
                            'amountPaid': amount_paid,
                            'amountCredited': amount_to_credit,
                            'isPremium': is_premium
                        }
                    }
                    mongo.db.corporate_revenue.insert_one(corporate_revenue)
                    print(f'💰 Corporate revenue recorded: ₦{deposit_fee} from user {user_id}')
                
                # Send notification
                try:
                    notification_id = create_user_notification(
                        mongo=mongo,
                        user_id=user_id,
                        category='wallet',
                        title='💰 Wallet Funded Successfully',
                        body=f'₦{amount_to_credit:,.2f} added to your Liquid Wallet. New balance: ₦{new_balance:,.2f}',
                        related_id=transaction_reference,
                        metadata={
                            'transaction_type': 'WALLET_FUNDING',
                            'amount_credited': amount_to_credit,
                            'deposit_fee': deposit_fee,
                            'new_balance': new_balance,
                            'is_premium': is_premium
                        },
                        priority='normal'
                    )
                    
                    if notification_id:
                        print(f'🔔 Wallet funding notification created: {notification_id}')
                except Exception as e:
                    print(f'⚠️ Failed to create notification: {str(e)}')
                
                print(f'✅ Wallet Funding: User {user_id}, Paid: ₦{amount_paid}, Fee: ₦{deposit_fee}, Credited: ₦{amount_to_credit}, New Balance: ₦{new_balance}')
                return jsonify({'success': True, 'message': 'Wallet funded successfully'}), 200
                
            except Exception as e:
                print(f'❌ Error processing wallet funding: {str(e)}')
                return jsonify({'success': False, 'message': 'Processing failed'}), 500
        
        def process_reserved_account_funding_update_only(user_id, amount_paid, transaction_reference, webhook_data):
            """Update wallet balance for existing transaction (no insert)"""
            try:
                wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
                if not wallet:
                    print(f'❌ Wallet not found for user: {user_id}')
                    return jsonify({'success': False, 'message': 'Wallet not found'}), 404
                
                # Check if user is premium (no deposit fee)
                user = mongo.db.users.find_one({'_id': ObjectId(user_id)})
                is_premium = False
                if user:
                    # CRITICAL FIX: Check multiple premium indicators
                    # 1. Check subscriptionStatus (standard subscription)
                    subscription_status = user.get('subscriptionStatus')
                    if subscription_status == 'active':
                        is_premium = True
                    
                    # 2. Check subscription dates (admin granted or standard)
                    elif user.get('subscriptionStartDate') and user.get('subscriptionEndDate'):
                        subscription_end = user.get('subscriptionEndDate')
                        now = datetime.utcnow()
                        if subscription_end > now:
                            is_premium = True
                            print(f'✅ User {user_id} is premium via subscription dates (ends: {subscription_end})')
                    
                    # 3. Check if user is admin
                    elif user.get('isAdmin', False):
                        is_premium = True
                        print(f'✅ User {user_id} is premium via admin status')
                
                print(f'💰 Premium check for user {user_id}: {is_premium}')
                
                # Apply deposit fee (₦30 for non-premium users)
                deposit_fee = 0.0 if is_premium else VAS_TRANSACTION_FEE
                amount_to_credit = amount_paid - deposit_fee
                
                # Ensure we don't credit negative amounts
                if amount_to_credit <= 0:
                    print(f'⚠️ Amount too small after fee: ₦{amount_paid} - ₦{deposit_fee} = ₦{amount_to_credit}')
                    return jsonify({'success': False, 'message': 'Amount too small to process'}), 400
                
                # Update wallet balance
                new_balance = wallet.get('balance', 0.0) + amount_to_credit
                
                mongo.db.vas_wallets.update_one(
                    {'userId': ObjectId(user_id)},
                    {'$set': {'balance': new_balance, 'updatedAt': datetime.utcnow()}}
                )
                
                # Record corporate revenue (₦30 fee)
                if deposit_fee > 0:
                    corporate_revenue = {
                        '_id': ObjectId(),
                        'type': 'SERVICE_FEE',
                        'category': 'DEPOSIT_FEE',
                        'amount': deposit_fee,
                        'userId': ObjectId(user_id),
                        'relatedTransaction': transaction_reference,
                        'description': f'Deposit fee from user {user_id}',
                        'status': 'RECORDED',
                        'createdAt': datetime.utcnow(),
                        'metadata': {
                            'amountPaid': amount_paid,
                            'amountCredited': amount_to_credit,
                            'isPremium': is_premium
                        }
                    }
                    mongo.db.corporate_revenue.insert_one(corporate_revenue)
                    print(f'💰 Corporate revenue recorded: ₦{deposit_fee} from user {user_id}')
                
                # Send notification
                try:
                    notification_id = create_user_notification(
                        mongo=mongo,
                        user_id=user_id,
                        category='wallet',
                        title='💰 Wallet Funded Successfully',
                        body=f'₦{amount_to_credit:,.2f} added to your Liquid Wallet. New balance: ₦{new_balance:,.2f}',
                        related_id=transaction_reference,
                        metadata={
                            'transaction_type': 'WALLET_FUNDING',
                            'amount_credited': amount_to_credit,
                            'deposit_fee': deposit_fee,
                            'new_balance': new_balance,
                            'is_premium': is_premium
                        },
                        priority='normal'
                    )
                    
                    if notification_id:
                        print(f'🔔 Wallet funding notification created: {notification_id}')
                except Exception as e:
                    print(f'⚠️ Failed to create notification: {str(e)}')
                
                print(f'✅ Wallet Funding (Update): User {user_id}, Paid: ₦{amount_paid}, Fee: ₦{deposit_fee}, Credited: ₦{amount_to_credit}, New Balance: ₦{new_balance}')
                return jsonify({'success': True, 'message': 'Wallet funded successfully'}), 200
                
            except Exception as e:
                print(f'❌ Error updating wallet funding: {str(e)}')
                return jsonify({'success': False, 'message': 'Processing failed'}), 500
        
        try:
            # Optional: IP Whitelisting (uncomment for production)
            # Monnify webhook IP: 35.242.133.146
            # client_ip = request.headers.get('X-Real-IP', request.remote_addr)
            # MONNIFY_WEBHOOK_IP = '35.242.133.146'
            # if client_ip != MONNIFY_WEBHOOK_IP:
            #     print(f'⚠️ Unauthorized webhook IP: {client_ip}')
            #     return jsonify({'success': False, 'message': 'Unauthorized'}), 403
            
            signature = request.headers.get('monnify-signature', '')
            payload = request.get_data(as_text=True)
            
            # CRITICAL: Verify webhook signature to prevent fake payments
            computed_signature = hmac.new(
                MONNIFY_SECRET_KEY.encode(),
                payload.encode(),
                hashlib.sha512
            ).hexdigest()
            
            if signature != computed_signature:
                print(f'⚠️ Invalid webhook signature. Expected: {computed_signature}, Got: {signature}')
                return jsonify({'success': False, 'message': 'Invalid signature'}), 401
            
            data = request.json
            
            # Log the raw webhook data for debugging
            print(f'📥 Raw Monnify webhook data: {json.dumps(data, indent=2)}')
            
            # Handle both old eventType format and new flat format
            event_type = data.get('eventType')
            payment_status = data.get('paymentStatus', '').upper()
            completed = data.get('completed', False)
            
            print(f'📥 Monnify webhook - EventType: {event_type}, Status: {payment_status}, Completed: {completed}')
            
            # Process if it's a successful transaction (either format)
            should_process = (
                (event_type == 'SUCCESSFUL_TRANSACTION') or 
                (payment_status == 'PAID' and completed)
            )
            
            if should_process:
                # ────────────────────────────────────────────────────────────────
                # 🚨 CRITICAL FIX: Check if this is a VAS confirmation first
                # ────────────────────────────────────────────────────────────────
                
                # Extract transaction reference for VAS detection
                transaction_reference = ''
                if 'eventData' in data:
                    transaction_reference = data['eventData'].get('transactionReference', '')
                else:
                    transaction_reference = data.get('transactionReference', '')
                
                print(f"🔍 Checking if webhook is for VAS transaction: {transaction_reference}")
                
                # Check if this webhook is for an existing VAS transaction (airtime/data)
                existing_vas_txn = mongo.db.vas_transactions.find_one({
                    '$or': [
                        {'requestId': transaction_reference},
                        {'transactionReference': transaction_reference}
                    ],
                    'type': {'$in': ['AIRTIME', 'DATA']}
                })
                
                if existing_vas_txn:
                    # This is a VAS confirmation - update existing transaction, don't create new one
                    print(f'📱 VAS confirmation webhook detected for: {transaction_reference}')
                    print(f'   Transaction ID: {existing_vas_txn["_id"]}')
                    print(f'   Type: {existing_vas_txn.get("type")}')
                    print(f'   Current Status: {existing_vas_txn.get("status")}')
                    
                    # Update existing transaction with webhook confirmation
                    update_data = {
                        'providerConfirmed': True,
                        'webhookReceived': datetime.utcnow(),
                        'webhookData': data,
                        'updatedAt': datetime.utcnow()
                    }
                    
                    # If transaction is still PENDING, update to SUCCESS
                    if existing_vas_txn.get('status') == 'PENDING':
                        update_data['status'] = 'SUCCESS'
                        print(f'✅ Updated PENDING VAS transaction to SUCCESS: {transaction_reference}')
                    
                    mongo.db.vas_transactions.update_one(
                        {'_id': existing_vas_txn['_id']},
                        {'$set': update_data}
                    )
                    
                    print(f'✅ VAS confirmation processed - no duplicate transaction created')
                    return jsonify({'success': True, 'message': 'VAS confirmation processed'}), 200
                
                # If we reach here, it's not a VAS confirmation - proceed with wallet funding logic
                print(f'💰 Processing as wallet funding (not VAS confirmation)')
                
                # ────────────────────────────────────────────────────────────────
                # IMPROVED EXTRACTION - handles real Monnify reserved account format
                # ────────────────────────────────────────────────────────────────
                # Default values
                account_ref = None
                amount_paid = 0.0
                transaction_reference = ''
                payment_reference = ''
                customer_email = ''
                
                print(f"DEBUG full payload top-level keys: {list(data.keys())}")
                
                # 1. Classic Monnify format (most common for reserved accounts)
                if 'eventData' in data:
                    event_data = data['eventData']
                    print(f"DEBUG eventData keys: {list(event_data.keys())}")
                    
                    amount_paid = float(event_data.get('amountPaid', 0))
                    transaction_reference = event_data.get('transactionReference', '')
                    payment_reference = event_data.get('paymentReference', '')
                    
                    # Customer email (fallback)
                    customer = event_data.get('customer', {})
                    customer_email = customer.get('email', '')
                    
                    # Critical: account reference is usually here
                    product = event_data.get('product', {})
                    if product.get('type') == 'RESERVED_ACCOUNT':
                        account_ref = product.get('reference', '')
                        print(f"DEBUG: Found reserved account reference → eventData.product.reference = '{account_ref}'")
                
                # 2. Possible flat/newer format (less common, but we check anyway)
                if not account_ref:
                    account_ref = data.get('accountReference', '')
                    if account_ref:
                        print(f"DEBUG: Found top-level accountReference = '{account_ref}'")
                        amount_paid = float(data.get('amountPaid', amount_paid))
                        transaction_reference = data.get('transactionReference', transaction_reference)
                        payment_reference = data.get('paymentReference', payment_reference)
                        customer_email = data.get('customerEmail', customer_email) or data.get('customer', {}).get('email', '')
                
                # 3. Log what we actually got
                print(f"DEBUG extracted values:")
                print(f"  - amount_paid          : ₦{amount_paid}")
                print(f"  - transaction_reference: {transaction_reference}")
                print(f"  - payment_reference    : {payment_reference}")
                print(f"  - account_ref          : '{account_ref}'")
                print(f"  - customer_email       : {customer_email}")
                
                if amount_paid <= 0:
                    print("⚠️ Zero or negative amount - ignoring")
                    return jsonify({'success': True, 'message': 'Zero amount ignored'}), 200
                
                # ────────────────────────────────────────────────────────────────
                # Now try to identify user and process
                # ────────────────────────────────────────────────────────────────
                user_id = None
                pending_txn = None
                
                # Priority 1: From account reference (preferred for reserved accounts)
                if account_ref:
                    cleaned = account_ref.replace(" ", "").replace("-", "").replace("_", "").upper()
                    if cleaned.startswith('FICORE'):
                        user_part = cleaned[len('FICORE'):]
                        user_id = user_part.lstrip('0123456789') if user_part.isdigit() else user_part
                        print(f"✅ Matched FICORE prefix → extracted user_id: {user_id}")
                
                # Priority 2: Fallback to email if we have it and no user yet
                if not user_id and customer_email:
                    user_doc = mongo.db.users.find_one({'email': customer_email})
                    if user_doc:
                        user_id = str(user_doc['_id'])
                        print(f"✅ Fallback: found user via email {customer_email} → {user_id}")
                
                # Priority 3: Try pending transaction matching (KYC payments only)
                if not user_id:
                    # Only check for KYC verification payments (₦70)
                    if amount_paid >= 70.0:
                        pending_txn = mongo.db.vas_transactions.find_one({
                            'monnifyTransactionReference': transaction_reference,
                            'status': 'PENDING_PAYMENT',
                            'type': 'KYC_VERIFICATION'
                        })
                        
                        if not pending_txn and payment_reference and payment_reference.startswith('VER_'):
                            pending_txn = mongo.db.vas_transactions.find_one({
                                'paymentReference': payment_reference,
                                'status': 'PENDING_PAYMENT',
                                'type': 'KYC_VERIFICATION'
                            })
                        
                        if not pending_txn and transaction_reference.startswith('FICORE_QP_'):
                            pending_txn = mongo.db.vas_transactions.find_one({
                                'transactionReference': transaction_reference,
                                'status': 'PENDING_PAYMENT',
                                'type': 'KYC_VERIFICATION'
                            })
                        
                        if pending_txn:
                            user_id = str(pending_txn['userId'])
                            print(f"✅ Found pending KYC verification transaction → user_id: {user_id}")
                
                # ────────────────────────────────────────────────────────────────
                # Decide how to process based on what we found
                # ────────────────────────────────────────────────────────────────
                if user_id:
                    # We have a user → treat as wallet funding (reserved account style)
                    print(f"Processing as direct reserved account funding for user {user_id}")
                    
                    # Comprehensive idempotency check - any status
                    existing = mongo.db.vas_transactions.find_one({
                        'reference': transaction_reference
                    })
                    
                    if existing:
                        if existing.get('status') == 'SUCCESS':
                            print(f"Duplicate SUCCESS webhook ignored: {transaction_reference}")
                            return jsonify({'success': True, 'message': 'Already processed'}), 200
                        else:
                            print(f"Found existing transaction with status {existing.get('status')}: {transaction_reference}")
                            print("Updating existing transaction to SUCCESS and crediting wallet...")
                            
                            # Update existing transaction to SUCCESS
                            mongo.db.vas_transactions.update_one(
                                {'_id': existing['_id']},
                                {'$set': {
                                    'status': 'SUCCESS',
                                    'amountPaid': amount_paid,
                                    'provider': 'monnify',
                                    'metadata': data,
                                    'completedAt': datetime.utcnow()
                                }}
                            )
                            
                            # Now credit the wallet (call the inline function but skip the insert part)
                            return process_reserved_account_funding_update_only(user_id, amount_paid, transaction_reference, data)
                    
                    return process_reserved_account_funding_inline(user_id, amount_paid, transaction_reference, data)
                
                elif pending_txn:
                    # KYC verification transaction
                    txn_type = pending_txn.get('type')
                    print(f"Found pending transaction type: {txn_type}")
                    
                    if txn_type == 'KYC_VERIFICATION':
                        # Process KYC verification payment
                        if amount_paid < 70.0:
                            print(f'⚠️ KYC verification payment insufficient: ₦{amount_paid} < ₦70')
                            return jsonify({'success': False, 'message': 'Insufficient payment amount'}), 400
                        
                        # Update transaction status
                        mongo.db.vas_transactions.update_one(
                            {'_id': pending_txn['_id']},
                            {'$set': {
                                'status': 'SUCCESS',
                                'amountPaid': amount_paid,
                                'reference': transaction_reference,
                                'provider': 'monnify',
                                'metadata': data,
                                'completedAt': datetime.utcnow()
                            }}
                        )
                        
                        # Record corporate revenue (₦70 KYC fee)
                        corporate_revenue = {
                            '_id': ObjectId(),
                            'type': 'SERVICE_FEE',
                            'category': 'KYC_VERIFICATION',
                            'amount': 70.0,
                            'userId': ObjectId(user_id),
                            'relatedTransaction': transaction_reference,
                            'description': f'KYC verification fee from user {user_id}',
                            'status': 'RECORDED',
                            'createdAt': datetime.utcnow(),
                            'metadata': {
                                'amountPaid': amount_paid,
                                'verificationFee': 70.0
                            }
                        }
                        mongo.db.corporate_revenue.insert_one(corporate_revenue)
                        print(f'💰 KYC verification revenue recorded: ₦70 from user {user_id}')
                        
                        print(f'✅ KYC Verification Payment: User {user_id}, Paid: ₦{amount_paid}, Fee: ₦70')
                        return jsonify({'success': True, 'message': 'KYC verification payment processed successfully'}), 200
                    
                    elif txn_type == 'WALLET_FUNDING':
                        return process_reserved_account_funding_inline(str(pending_txn['userId']), amount_paid, transaction_reference, data)
                    
                    else:
                        print(f"Unhandled pending txn type: {txn_type}")
                        return jsonify({'success': False, 'message': 'Unhandled transaction type'}), 400
                
                else:
                    print("Could not identify user or pending transaction")
                    # Still return 200 to Monnify - don't block their retries
                    return jsonify({'success': True, 'message': 'Acknowledged but unprocessed'}), 200
            
            # If payment status is not PAID or not completed, just acknowledge
            else:
                print(f'📥 Webhook received but not processed - Status: {payment_status}, Completed: {completed}')
                return jsonify({'success': True, 'message': 'Webhook received'}), 200
            
        except Exception as e:
            print(f'❌ Error processing webhook: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Webhook processing failed',
                'errors': {'general': [str(e)]}
            }), 500
    
    # ==================== VAS PURCHASE ENDPOINTS ====================
    
    @vas_bp.route('/buy-airtime', methods=['POST'])
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
                    'message': 'Amount must be between ₦100 and ₦5,000'
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
            
            # 🚨 EMERGENCY PRICING DETECTION
            emergency_multiplier = 2.0
            normal_expected_cost = amount * 0.99  # Expected normal cost for airtime
            is_emergency_pricing = cost_price >= (normal_expected_cost * emergency_multiplier * 0.8)  # 80% threshold
            
            if is_emergency_pricing:
                print(f"🚨 EMERGENCY PRICING DETECTED: Cost ₦{cost_price} vs Expected ₦{normal_expected_cost}")
                # Will tag after successful transaction
            
            # CRITICAL: Check for pending duplicate transaction (idempotency)
            pending_txn = check_pending_transaction(user_id, 'AIRTIME', selling_price, phone_number)
            if pending_txn:
                print(f'⚠️ Duplicate airtime request blocked for user {user_id}')
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
                    'message': f'Insufficient wallet balance. Required: ₦{total_amount:.2f}, Available: ₦{wallet.get("balance", 0.0):.2f}'
                }), 400
            
            # Generate unique request ID
            request_id = generate_request_id(user_id, 'AIRTIME')
            
            # Create PENDING transaction first (idempotency lock)
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
                'status': 'PENDING',
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
                print(f'✅ Monnify airtime purchase successful: {request_id}')
            except Exception as monnify_error:
                print(f'⚠️ Monnify failed: {str(monnify_error)}')
                error_message = str(monnify_error)
                
                try:
                    # Fallback to Peyflex
                    api_response = call_peyflex_airtime(network, amount, phone_number, request_id)
                    provider = 'peyflex'
                    success = True
                    print(f'✅ Peyflex airtime purchase successful (fallback): {request_id}')
                except Exception as peyflex_error:
                    print(f'❌ Peyflex failed: {str(peyflex_error)}')
                    error_message = f'Both providers failed. Monnify: {monnify_error}, Peyflex: {peyflex_error}'
            
            if not success:
                # Update transaction to FAILED
                mongo.db.vas_transactions.update_one(
                    {'_id': transaction_id},
                    {'$set': {'status': 'FAILED', 'errorMessage': error_message, 'updatedAt': datetime.utcnow()}}
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
            
            # Update transaction to SUCCESS
            mongo.db.vas_transactions.update_one(
                {'_id': transaction_id},
                {
                    '$set': {
                        'status': 'SUCCESS',
                        'provider': provider,
                        'providerResponse': api_response,
                        'updatedAt': datetime.utcnow()
                    }
                }
            )
            
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
                print(f'💰 Corporate revenue recorded: ₦{margin} from airtime sale to user {user_id}')
            
            # 🚨 TAG EMERGENCY TRANSACTIONS FOR RECOVERY
            if is_emergency_pricing:
                try:
                    emergency_tag_id = tag_emergency_transaction(
                        mongo.db, str(transaction_id), cost_price, 'airtime', network
                    )
                    print(f'🏥 Emergency transaction tagged for recovery: {emergency_tag_id}')
                    
                    # Create immediate notification about emergency pricing
                    create_user_notification(
                        mongo=mongo.db,
                        user_id=user_id,
                        category='system',
                        title='⚡ Emergency Pricing Used',
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
                    print(f'⚠️ Failed to tag emergency transaction: {str(e)}')
                    # Don't fail the transaction if tagging fails
            
            # Auto-create expense entry (auto-bookkeeping)
            base_description = f'Airtime - {network} ₦{amount} for {phone_number[-4:]}****'
            
            # 🎯 PASSIVE RETENTION ENGINE: Generate retention-focused description
            retention_description = generate_retention_description(
                base_description,
                savings_message,
                user_tier,
                pricing_result.get('discount_applied', 0)
            )
            
            expense_entry = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'amount': selling_price,  # Record selling price as expense
                'category': 'Utilities',
                'description': retention_description,  # Use retention-enhanced description
                'date': datetime.utcnow(),
                'tags': ['VAS', 'Airtime', network],
                'vasTransactionId': transaction_id,
                'metadata': {
                    'faceValue': amount,
                    'actualCost': selling_price,
                    'userTier': user_tier,
                    'savingsMessage': savings_message,
                    'originalPrice': pricing_result.get('cost_price', 0) + pricing_result.get('margin', 0),
                    'discountApplied': pricing_result.get('discount_applied', 0),
                    'pricingStrategy': pricing_result.get('strategy_used', 'standard'),
                    'freeFeesApplied': pricing_result.get('free_fee_applied', False),
                    'baseDescription': base_description,  # Store original for reference
                    'retentionEnhanced': True  # Flag to indicate retention messaging applied
                },
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.expenses.insert_one(expense_entry)
            
            print(f'✅ Airtime purchase complete: User {user_id}, Face Value: ₦{amount}, Charged: ₦{selling_price}, Margin: ₦{margin}, Provider: {provider}')
            
            # 🎯 RETENTION DATA for Frontend Trust Building
            retention_data = {
                'userTier': user_tier,
                'originalPrice': amount,
                'finalPrice': selling_price,
                'totalSaved': amount - selling_price,
                'savingsMessage': savings_message,
                'subscriptionROI': {
                    'tierName': user_tier.title() if user_tier != 'basic' else 'Basic',
                    'annualCost': 25000 if user_tier == 'gold' else (10000 if user_tier == 'premium' else 0),
                    'monthlyProgress': f"You've saved ₦{amount - selling_price:.0f} this transaction",
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
                    'phoneNumber': phone_number,  # 🔧 FIX: Include phone number in response
                    'network': network,  # 🔧 FIX: Include network in response
                    'faceValue': amount,
                    'amountCharged': selling_price,
                    'margin': margin,
                    'newBalance': new_balance,
                    'provider': provider,
                    'userTier': user_tier,
                    'savingsMessage': savings_message,
                    'pricingStrategy': pricing_result['strategy_used'],
                    'expenseRecorded': True,
                    'retentionData': retention_data  # 🎯 NEW: Frontend trust data
                },
                'message': f'Airtime purchased successfully! {savings_message}' if savings_message else 'Airtime purchased successfully!'
            }), 200
            
        except Exception as e:
            print(f'❌ Error buying airtime: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to purchase airtime',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/buy-data', methods=['POST'])
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
            
            # Calculate dynamic pricing
            pricing_result = calculate_vas_price(
                mongo.db, 'data', network, amount, user_tier, data_plan_id, user_id
            )
            
            selling_price = pricing_result['selling_price']
            cost_price = pricing_result['cost_price']
            margin = pricing_result['margin']
            savings_message = pricing_result['savings_message']
            
            # 🚨 EMERGENCY PRICING DETECTION
            emergency_multiplier = 2.0
            normal_expected_cost = amount  # For data, amount is usually the expected cost
            is_emergency_pricing = cost_price >= (normal_expected_cost * emergency_multiplier * 0.8)  # 80% threshold
            
            if is_emergency_pricing:
                print(f"🚨 EMERGENCY PRICING DETECTED: Cost ₦{cost_price} vs Expected ₦{normal_expected_cost}")
                # Will tag after successful transaction
            
            # CRITICAL: Check for pending duplicate transaction (idempotency)
            pending_txn = check_pending_transaction(user_id, 'DATA', selling_price, phone_number)
            if pending_txn:
                print(f'⚠️ Duplicate data request blocked for user {user_id}')
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
                    'message': f'Insufficient wallet balance. Required: ₦{total_amount:.2f}, Available: ₦{wallet.get("balance", 0.0):.2f}'
                }), 400
            
            # Generate unique request ID
            request_id = generate_request_id(user_id, 'DATA')
            
            # Create PENDING transaction first (idempotency lock)
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
                'pricingStrategy': pricing_result['strategy_used'],
                'savingsMessage': savings_message,
                'totalAmount': total_amount,
                'status': 'PENDING',
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
                api_response = call_monnify_data(network, data_plan_id, phone_number, request_id)
                success = True
                print(f'✅ Monnify data purchase successful: {request_id}')
            except Exception as monnify_error:
                print(f'⚠️ Monnify failed: {str(monnify_error)}')
                error_message = str(monnify_error)
                
                try:
                    # Fallback to Peyflex
                    api_response = call_peyflex_data(network, data_plan_id, phone_number, request_id)
                    provider = 'peyflex'
                    success = True
                    print(f'✅ Peyflex data purchase successful (fallback): {request_id}')
                except Exception as peyflex_error:
                    print(f'❌ Peyflex failed: {str(peyflex_error)}')
                    error_message = f'Both providers failed. Monnify: {monnify_error}, Peyflex: {peyflex_error}'
            
            if not success:
                # Update transaction to FAILED
                mongo.db.vas_transactions.update_one(
                    {'_id': transaction_id},
                    {'$set': {'status': 'FAILED', 'errorMessage': error_message, 'updatedAt': datetime.utcnow()}}
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
            
            # Update transaction to SUCCESS
            mongo.db.vas_transactions.update_one(
                {'_id': transaction_id},
                {
                    '$set': {
                        'status': 'SUCCESS',
                        'provider': provider,
                        'providerResponse': api_response,
                        'updatedAt': datetime.utcnow()
                    }
                }
            )
            
            # Record corporate revenue (margin earned)
            if margin > 0:
                corporate_revenue = {
                    '_id': ObjectId(),
                    'type': 'VAS_MARGIN',
                    'category': 'DATA_MARGIN',
                    'amount': margin,
                    'userId': ObjectId(user_id),
                    'relatedTransaction': str(transaction_id),
                    'description': f'Data margin from user {user_id} - {network} {data_plan_name}',
                    'status': 'RECORDED',
                    'createdAt': datetime.utcnow(),
                    'metadata': {
                        'network': network,
                        'planName': data_plan_name,
                        'planId': data_plan_id,
                        'originalAmount': amount,
                        'sellingPrice': selling_price,
                        'costPrice': cost_price,
                        'userTier': user_tier,
                        'strategy': pricing_result['strategy_used'],
                        'emergencyPricing': is_emergency_pricing
                    }
                }
                mongo.db.corporate_revenue.insert_one(corporate_revenue)
                print(f'💰 Corporate revenue recorded: ₦{margin} from data sale to user {user_id}')
            
            # 🚨 TAG EMERGENCY TRANSACTIONS FOR RECOVERY
            if is_emergency_pricing:
                try:
                    emergency_tag_id = tag_emergency_transaction(
                        mongo.db, str(transaction_id), cost_price, 'data', network
                    )
                    print(f'🏥 Emergency transaction tagged for recovery: {emergency_tag_id}')
                    
                    # Create immediate notification about emergency pricing
                    create_user_notification(
                        mongo=mongo.db,
                        user_id=user_id,
                        category='system',
                        title='⚡ Emergency Pricing Used',
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
                    print(f'⚠️ Failed to tag emergency transaction: {str(e)}')
                    # Don't fail the transaction if tagging fails
            
            # 🎯 PASSIVE RETENTION ENGINE: Generate retention-focused description
            base_description = f'Data - {network} {data_plan_name} for {phone_number[-4:]}****'
            discount_applied = amount - selling_price  # Calculate actual discount
            retention_description = generate_retention_description(
                base_description,
                savings_message,
                user_tier,
                discount_applied
            )
            
            # Auto-create expense entry (auto-bookkeeping)
            expense_entry = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'amount': selling_price,  # Record selling price as expense
                'category': 'Utilities',
                'description': retention_description,  # 🎯 Use retention-focused description
                'date': datetime.utcnow(),
                'tags': ['VAS', 'Data', network],
                'vasTransactionId': transaction_id,
                'metadata': {
                    'planName': data_plan_name,
                    'originalAmount': amount,
                    'actualCost': selling_price,
                    'userTier': user_tier,
                    'savingsMessage': savings_message,
                    'discountApplied': discount_applied,  # Track discount for analytics
                    'retentionMessaging': True  # Flag for retention analytics
                },
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.expenses.insert_one(expense_entry)
            
            # 🎯 RETENTION DATA for Frontend Trust Building
            retention_data = {
                'userTier': user_tier,
                'originalPrice': amount,
                'finalPrice': selling_price,
                'totalSaved': discount_applied,
                'savingsMessage': savings_message,
                'subscriptionROI': {
                    'tierName': user_tier.title() if user_tier != 'basic' else 'Basic',
                    'annualCost': 25000 if user_tier == 'gold' else (10000 if user_tier == 'premium' else 0),
                    'monthlyProgress': f"You've saved ₦{discount_applied:.0f} this transaction",
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

            print(f'✅ Data purchase complete: User {user_id}, Plan: {data_plan_name}, Original: ₦{amount}, Charged: ₦{selling_price}, Margin: ₦{margin}, Provider: {provider}')
            
            return jsonify({
                'success': True,
                'data': {
                    'transactionId': str(transaction_id),
                    'requestId': request_id,
                    'phoneNumber': phone_number,  # 🔧 FIX: Include phone number in response
                    'network': network,  # 🔧 FIX: Include network in response
                    'planName': data_plan_name,
                    'originalAmount': amount,
                    'amountCharged': selling_price,
                    'margin': margin,
                    'newBalance': new_balance,
                    'provider': provider,
                    'userTier': user_tier,
                    'savingsMessage': savings_message,
                    'pricingStrategy': pricing_result['strategy_used'],
                    'expenseRecorded': True,
                    'retentionData': retention_data  # 🎯 NEW: Frontend trust data
                },
                'message': f'Data purchased successfully! {savings_message}' if savings_message else 'Data purchased successfully!'
            }), 200
            
        except Exception as e:
            print(f'❌ Error buying data: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to purchase data',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/networks/data', methods=['GET'])
    @token_required
    def get_data_networks(current_user):
        """Get available data networks from Monnify Bills API (primary) with Peyflex fallback"""
        try:
            print('🔍 Fetching data networks from Monnify Bills API')
            
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
                
                print(f'✅ Successfully retrieved {len(networks)} data networks from Monnify')
                return jsonify({
                    'success': True,
                    'data': networks,
                    'message': 'Data networks retrieved from Monnify Bills API',
                    'source': 'monnify_bills'
                }), 200
                
            except Exception as monnify_error:
                print(f'⚠️ Monnify data networks failed: {str(monnify_error)}')
                
                # Fallback to Peyflex
                print('🔄 Falling back to Peyflex for data networks')
                
                headers = {
                    'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'FiCore-Backend/1.0'
                }
                
                url = f'{PEYFLEX_BASE_URL}/api/data/networks/'
                print(f'📡 Calling Peyflex networks API: {url}')
                
                try:
                    response = requests.get(url, headers=headers, timeout=30)
                    print(f'📥 Peyflex networks response status: {response.status_code}')
                    
                    if response.status_code == 200:
                        peyflex_data = response.json()
                        if peyflex_data.get('success'):
                            networks = peyflex_data.get('data', [])
                            
                            print(f'✅ Successfully retrieved {len(networks)} data networks from Peyflex')
                            return jsonify({
                                'success': True,
                                'data': networks,
                                'message': 'Data networks retrieved from Peyflex (fallback)',
                                'source': 'peyflex'
                            }), 200
                    
                    print(f'⚠️ Peyflex data networks failed with status: {response.status_code}')
                    
                except Exception as peyflex_error:
                    print(f'⚠️ Peyflex data networks error: {str(peyflex_error)}')
                
                # If both fail, return default networks
                default_networks = [
                    {'id': 'mtn', 'name': 'MTN', 'code': 'MTN', 'source': 'default'},
                    {'id': 'glo', 'name': 'Glo', 'code': 'GLO', 'source': 'default'},
                    {'id': 'airtel', 'name': 'Airtel', 'code': 'AIRTEL', 'source': 'default'},
                    {'id': '9mobile', 'name': '9mobile', 'code': '9MOBILE', 'source': 'default'}
                ]
                
                return jsonify({
                    'success': True,
                    'data': default_networks,
                    'message': 'Default data networks (providers unavailable)',
                    'source': 'default'
                }), 200
                
        except Exception as e:
            print(f'❌ Error fetching data networks: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to fetch data networks',
                'errors': {'general': [str(e)]}
            }), 500

    @vas_bp.route('/reserved-account/add-linked-accounts', methods=['POST'])
    @token_required
    def add_linked_accounts(current_user):
        """Add additional bank accounts to existing reserved account for verified users"""
        try:
            user_id = str(current_user['_id'])
            data = request.get_json() or {}
            
            # Support both parameter formats for flexibility
            get_all_available_banks = data.get('getAllAvailableBanks', False)
            preferred_banks = data.get('preferredBanks', data.get('bankCodes', ['50515', '101']))
            
            print(f'🏦 Adding linked accounts for user {user_id}')
            print(f'🏦 getAllAvailableBanks: {get_all_available_banks}')
            print(f'🏦 preferredBanks: {preferred_banks}')
            
            # Get user's wallet
            user_doc = mongo.db.users.find_one({'_id': ObjectId(user_id)})
            if not user_doc:
                return jsonify({'success': False, 'message': 'User not found'}), 404
            
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if not wallet:
                return jsonify({'success': False, 'message': 'No wallet found. Please create one first.'}), 404
            
            reserved_account_ref = wallet.get('reservedAccountReference') or wallet.get('accountReference')
            if not reserved_account_ref:
                return jsonify({'success': False, 'message': 'No existing reserved account reference found.'}), 400
            
            # Gate: only allow for fully verified users (BVN + NIN present)
            if not user_doc.get('bvn'):
                return jsonify({
                    'success': False,
                    'message': 'BVN verification required before adding additional accounts'
                }), 400
            
            print(f'🏦 User has existing account reference: {reserved_account_ref}')
            print(f'🏦 User is verified with BVN: {user_doc.get("bvn", "")[:3]}***')
            
            # Check which banks are already present (avoid duplicate requests)
            existing_accounts = wallet.get('accounts', [])
            existing_bank_codes = {acc.get('bankCode') for acc in existing_accounts if acc.get('bankCode')}
            banks_to_add = [code for code in preferred_banks if code not in existing_bank_codes]
            
            if not banks_to_add and not get_all_available_banks:
                print("All requested banks already present")
                return jsonify({
                    'success': True,
                    'data': {
                        'added': [],
                        'alreadyPresent': list(existing_bank_codes),
                        'totalBanks': len(existing_accounts)
                    },
                    'message': 'All requested banks are already linked.'
                }), 200
            
            # Authenticate with Monnify
            monnify_token = get_monnify_token()
            if not monnify_token:
                return jsonify({
                    'success': False,
                    'message': 'Failed to authenticate with payment provider'
                }), 500
            
            # Use the CORRECT Monnify API URL and payload structure from their docs
            url = f"{MONNIFY_BASE_URL}/api/v1/bank-transfer/reserved-accounts/{reserved_account_ref}/add-linked-accounts"
            
            # Prepare payload according to Monnify documentation
            payload = {
                'getAllAvailableBanks': get_all_available_banks,
                'preferredBanks': preferred_banks if not get_all_available_banks else []
            }
            
            print(f'🏦 Calling Monnify: {url}')
            print(f'🏦 Payload: {payload}')
            
            headers = {
                'Authorization': f'Bearer {monnify_token}',
                'Content-Type': 'application/json'
            }
            
            # Use PUT method as shown in Monnify docs
            response = requests.put(url, headers=headers, json=payload, timeout=30)
            print(f'🏦 Monnify response status: {response.status_code}')
            print(f'🏦 Monnify response: {response.text}')
            
            if response.status_code == 200:
                monnify_data = response.json()
                
                # Check Monnify's response structure according to official docs
                if monnify_data.get('requestSuccessful'):
                    response_body = monnify_data.get('responseBody', {})
                    accounts = response_body.get('accounts', [])
                    
                    print(f'🏦 Monnify success: {len(accounts)} accounts returned')
                    
                    # Update wallet document with new accounts
                    mongo.db.vas_wallets.update_one(
                        {'userId': ObjectId(user_id)},
                        {
                            '$set': {
                                'accounts': accounts,
                                'updatedAt': datetime.utcnow()
                            }
                        }
                    )
                    
                    print(f'🏦 Successfully updated wallet with {len(accounts)} linked accounts')
                    
                    return jsonify({
                        'success': True,
                        'data': {
                            'accounts': accounts,
                            'totalBanksNow': len(accounts),
                            'message': f'Successfully added additional bank accounts'
                        },
                        'message': 'Additional bank accounts added successfully'
                    }), 200
                else:
                    # Handle Monnify API error according to official response structure
                    error_msg = monnify_data.get('responseMessage', 'Failed to add linked accounts')
                    error_code = monnify_data.get('responseCode', 'UNKNOWN')
                    print(f'🏦 Monnify API error: {error_code} - {error_msg}')
                    
                    return jsonify({
                        'success': False,
                        'message': f'Monnify API Error: {error_msg}',
                        'errorCode': error_code
                    }), 400
            else:
                print(f'🏦 Monnify API error: {response.status_code}')
                error_msg = response.text
                try:
                    error_data = response.json()
                    error_msg = error_data.get('responseMessage') or error_data.get('message') or error_msg
                except:
                    pass
                return jsonify({
                    'success': False,
                    'message': f'Failed to add additional bank accounts: {error_msg}'
                }), response.status_code
                
        except Exception as e:
            print(f'❌ Error adding linked accounts: {str(e)}')
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'message': 'Failed to add additional bank accounts',
                'error': str(e)
            }), 500
            
            monnify_data = response.json()
            if not monnify_data.get('requestSuccessful'):
                return jsonify({
                    'success': False,
                    'message': monnify_data.get('responseMessage', 'Unknown Monnify error')
                }), 400
            
            updated_accounts = monnify_data.get('responseBody', {}).get('accounts', [])
            if not updated_accounts:
                return jsonify({
                    'success': False,
                    'message': 'Monnify returned no accounts after adding'
                }), 500
            
            # Update wallet document with new accounts list
            mongo.db.vas_wallets.update_one(
                {'userId': ObjectId(user_id)},
                {
                    '$set': {
                        'accounts': updated_accounts,
                        'updatedAt': datetime.utcnow()
                    }
                }
            )
            
            print(f"Successfully added {len(banks_to_add)} linked accounts. Now has {len(updated_accounts)} banks.")
            
            return jsonify({
                'success': True,
                'data': {
                    'added': banks_to_add,
                    'totalBanksNow': len(updated_accounts),
                    'accounts': updated_accounts  # full updated list for frontend
                },
                'message': f'Successfully added {len(banks_to_add)} additional bank account(s).'
            }), 200
            
        except Exception as e:
            print(f"ERROR adding linked banks: {str(e)}")
            import traceback
            traceback.print_exc()
            return jsonify({
                'success': False,
                'message': 'Failed to add linked bank accounts',
                'error': str(e)
            }), 500
    
    @vas_bp.route('/reserved-account/transactions', methods=['GET'])
    @token_required
    def get_reserved_account_transactions(current_user):
        """Get user's reserved account transaction history (wallet funding transactions)"""
        try:
            user_id = str(current_user['_id'])
            
            limit = int(request.args.get('limit', 50))
            skip = int(request.args.get('skip', 0))
            
            # Get only WALLET_FUNDING transactions
            transactions = list(
                mongo.db.vas_transactions.find({
                    'userId': ObjectId(user_id),
                    'type': 'WALLET_FUNDING'
                })
                .sort('createdAt', -1)
                .skip(skip)
                .limit(limit)
            )
            
            serialized_transactions = []
            for txn in transactions:
                txn_data = serialize_doc(txn)
                # Ensure createdAt is a string for frontend compatibility
                txn_data['createdAt'] = txn.get('createdAt', datetime.utcnow()).isoformat() + 'Z'
                # Add reference and description for frontend display
                txn_data['reference'] = txn.get('reference', '')
                txn_data['description'] = f"Wallet Funding - ₦{txn.get('amount', 0):.2f}"
                serialized_transactions.append(txn_data)
            
            return jsonify({
                'success': True,
                'data': serialized_transactions,
                'message': 'Reserved account transactions retrieved successfully'
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting reserved account transactions: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve reserved account transactions',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/transactions/<transaction_id>/receipt', methods=['GET'])
    @token_required
    def get_vas_transaction_receipt(current_user, transaction_id):
        """Get VAS transaction receipt for display"""
        try:
            user_id = str(current_user['_id'])
            
            # Find the transaction
            transaction = mongo.db.vas_transactions.find_one({
                '_id': ObjectId(transaction_id),
                'userId': ObjectId(user_id)
            })
            
            if not transaction:
                return jsonify({
                    'success': False,
                    'message': 'Transaction not found'
                }), 404
            
            # Build receipt data based on transaction type
            txn_type = transaction.get('type', 'UNKNOWN')
            amount = transaction.get('amount', 0)
            status = transaction.get('status', 'UNKNOWN')
            reference = transaction.get('reference', 'N/A')
            created_at = transaction.get('createdAt', datetime.utcnow())
            provider = transaction.get('provider', 'N/A')
            metadata = transaction.get('metadata', {})
            
            receipt_data = {
                'transactionId': str(transaction['_id']),
                'type': txn_type,
                'amount': amount,
                'status': status,
                'reference': reference,
                'provider': provider,
                'date': created_at.isoformat() + 'Z',
                'metadata': metadata
            }
            
            # Add type-specific details
            if txn_type == 'WALLET_FUNDING':
                receipt_data.update({
                    'title': 'Wallet Funding Receipt',
                    'description': f'₦{amount:,.2f} added to your Liquid Wallet',
                    'details': {
                        'Amount Paid': f"₦{transaction.get('amountPaid', amount):,.2f}",
                        'Deposit Fee': f"₦{transaction.get('depositFee', 0):,.2f}",
                        'Amount Credited': f"₦{amount:,.2f}",
                        'Payment Method': 'Bank Transfer',
                        'Provider': provider.title()
                    }
                })
            elif txn_type == 'AIRTIME_PURCHASE':
                phone = metadata.get('phoneNumber', 'Unknown')
                network = metadata.get('network', 'Unknown')
                receipt_data.update({
                    'title': 'Airtime Purchase Receipt',
                    'description': f'₦{amount:,.2f} airtime sent successfully',
                    'details': {
                        'Phone Number': phone,
                        'Network': network,
                        'Amount': f"₦{amount:,.2f}",
                        'Face Value': f"₦{metadata.get('faceValue', amount):,.2f}",
                        'Provider': provider.title()
                    }
                })
            elif txn_type == 'DATA_PURCHASE':
                phone = metadata.get('phoneNumber', 'Unknown')
                network = metadata.get('network', 'Unknown')
                plan_name = metadata.get('planName', 'Data Plan')
                receipt_data.update({
                    'title': 'Data Purchase Receipt',
                    'description': f'{plan_name} purchased successfully',
                    'details': {
                        'Phone Number': phone,
                        'Network': network,
                        'Data Plan': plan_name,
                        'Amount': f"₦{amount:,.2f}",
                        'Provider': provider.title()
                    }
                })
            elif txn_type == 'KYC_VERIFICATION':
                receipt_data.update({
                    'title': 'KYC Verification Receipt',
                    'description': 'Account verification completed',
                    'details': {
                        'Verification Fee': f"₦{amount:,.2f}",
                        'Status': 'Verified',
                        'Provider': provider.title()
                    }
                })
            else:
                # Use the same helper function for receipt descriptions
                description, _ = get_transaction_display_info(transaction)
                
                receipt_data.update({
                    'title': f'{txn_type.replace("_", " ").title()} Receipt',
                    'description': description,
                    'details': {
                        'Amount': f"₦{amount:,.2f}",
                        'Type': txn_type.replace("_", " ").title(),
                        'Provider': provider.title()
                    }
                })
            
            return jsonify({
                'success': True,
                'data': receipt_data,
                'message': 'Transaction receipt retrieved successfully'
            })
            
        except Exception as e:
            print(f'❌ Error getting VAS receipt: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve transaction receipt',
                'errors': {'general': [str(e)]}
            }), 500

    # ==================== BILLS PAYMENT ENDPOINTS ====================
    
    @vas_bp.route('/bills/categories', methods=['GET'])
    @token_required
    def get_bill_categories(current_user):
        """Get available bill categories from Monnify Bills API"""
        try:
            print('🔍 Fetching bill categories from Monnify Bills API')
            
            access_token = get_monnify_access_token()
            response = call_monnify_bills_api(
                'biller-categories?size=50',
                'GET',
                access_token=access_token
            )
            
            print(f'📡 Monnify bill categories response: {response}')
            
            categories = []
            for category in response['responseBody']['content']:
                # Filter out categories we already handle (AIRTIME, DATA_BUNDLE)
                if category['code'] not in ['AIRTIME', 'DATA_BUNDLE']:
                    categories.append({
                        'id': category['code'].lower(),
                        'name': category['name'],
                        'code': category['code'],
                        'available': True,
                        'description': f"Pay {category['name'].lower().replace('_', ' ')} bills"
                    })
            
            print(f'✅ Processed {len(categories)} bill categories')
            
            return jsonify({
                'success': True,
                'data': categories,
                'message': 'Bill categories retrieved successfully',
                'source': 'monnify_bills'
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting bill categories: {str(e)}')
            return jsonify({
                'success': False,
                'message': f'Failed to get bill categories: {str(e)}',
                'errors': {'general': [str(e)]}
            }), 500

    @vas_bp.route('/bills/providers/<category>', methods=['GET'])
    @token_required
    def get_bill_providers(current_user, category):
        """Get bill providers for a specific category"""
        try:
            print(f'🔍 Fetching bill providers for category: {category}')
            
            # Map frontend category to Monnify category
            category_mapping = {
                'electricity': 'ELECTRICITY',
                'cable_tv': 'CABLE_TV',
                'water': 'WATER',
                'internet': 'INTERNET',
                'transportation': 'TRANSPORTATION'
            }
            
            monnify_category = category_mapping.get(category.lower())
            if not monnify_category:
                print(f'❌ Unsupported category: {category}')
                return jsonify({
                    'success': False,
                    'message': f'Unsupported category: {category}',
                    'errors': {'category': [f'Category {category} is not supported']}
                }), 400
            
            print(f'📡 Calling Monnify API for category: {monnify_category}')
            access_token = get_monnify_access_token()
            response = call_monnify_bills_api(
                f'billers?category_code={monnify_category}&size=100',
                'GET',
                access_token=access_token
            )
            
            print(f'📡 Monnify providers response for {monnify_category}: {response}')
            
            # DEBUGGING: Check if we're getting wrong providers for transportation
            if category.lower() == 'transportation':
                print(f'🚨 TRANSPORTATION DEBUG: Raw Monnify response: {json.dumps(response, indent=2)}')
                
                # Check if any providers contain electricity-related terms
                electricity_keywords = ['electricity', 'electric', 'distribution', 'disco', 'power', 'energy']
                raw_providers = response.get('responseBody', {}).get('content', [])
                
                electricity_providers = []
                for provider in raw_providers:
                    provider_name = provider.get('name', '').lower()
                    if any(keyword in provider_name for keyword in electricity_keywords):
                        electricity_providers.append(provider)
                
                if electricity_providers:
                    print(f'🚨 TRANSPORTATION ISSUE: Found {len(electricity_providers)} electricity providers in transportation category!')
                    print(f'🚨 Electricity providers: {[p.get("name") for p in electricity_providers]}')
                    print(f'🚨 This indicates Monnify API configuration issue - transportation category returning electricity providers')
                    
                    # Return error with detailed explanation
                    return jsonify({
                        'success': False,
                        'message': 'Transportation providers are misconfigured on the payment gateway',
                        'errors': {
                            'backend_issue': [
                                'Monnify API is returning electricity providers for transportation category',
                                'This is a payment gateway configuration issue, not an app issue',
                                f'Found {len(electricity_providers)} electricity providers in transportation response'
                            ]
                        },
                        'debug_info': {
                            'requested_category': category,
                            'monnify_category': monnify_category,
                            'total_providers': len(raw_providers),
                            'electricity_providers_found': len(electricity_providers),
                            'electricity_provider_names': [p.get('name') for p in electricity_providers]
                        }
                    }), 503  # Service Unavailable
            
            providers = []
            for biller in response['responseBody']['content']:
                providers.append({
                    'id': biller['code'],
                    'name': biller['name'],
                    'code': biller['code'],
                    'category': category,
                    'description': f"{biller['name']} - {category.replace('_', ' ').title()} provider"
                })
            
            print(f'✅ Processed {len(providers)} providers for {category}')
            
            return jsonify({
                'success': True,
                'data': providers,
                'message': f'Providers for {category} retrieved successfully',
                'source': 'monnify_bills',
                'category': category,
                'monnify_category': monnify_category
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting providers for {category}: {str(e)}')
            return jsonify({
                'success': False,
                'message': f'Failed to get providers for {category}: {str(e)}',
                'errors': {'general': [str(e)]}
            }), 500

    @vas_bp.route('/bills/products/<provider>', methods=['GET'])
    @token_required
    def get_bill_products(current_user, provider):
        """Get products/packages for a specific provider"""
        try:
            print(f'🔍 Fetching bill products for provider: {provider}')
            
            access_token = get_monnify_access_token()
            response = call_monnify_bills_api(
                f'biller-products?biller_code={provider}&size=100',
                'GET',
                access_token=access_token
            )
            
            print(f'📡 Monnify products response for {provider}: {response}')
            
            products = []
            for product in response['responseBody']['content']:
                # Extract metadata for better product information
                metadata = product.get('metadata', {})
                duration = metadata.get('duration', 1)
                duration_unit = metadata.get('durationUnit', 'MONTHLY')
                product_type = metadata.get('productType', {})
                
                # Format duration display
                duration_display = f"{duration} {duration_unit.lower()}" if duration_unit else "One-time"
                
                products.append({
                    'id': product['code'],
                    'name': product['name'],
                    'code': product['code'],
                    'price': product.get('price'),
                    'priceType': product.get('priceType', 'OPEN'),
                    'minAmount': product.get('minAmount'),
                    'maxAmount': product.get('maxAmount'),
                    'duration': duration_display,
                    'productType': product_type.get('name', 'Service'),
                    'description': f"{product['name']} - {duration_display}",
                    'metadata': metadata
                })
            
            print(f'✅ Processed {len(products)} products for {provider}')
            
            return jsonify({
                'success': True,
                'data': products,
                'message': f'Products for {provider} retrieved successfully',
                'source': 'monnify_bills',
                'provider': provider
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting products for {provider}: {str(e)}')
            return jsonify({
                'success': False,
                'message': f'Failed to get products for {provider}: {str(e)}',
                'errors': {'general': [str(e)]}
            }), 500

    @vas_bp.route('/bills/validate', methods=['POST'])
    @token_required
    def validate_bill_account(current_user):
        """Validate customer account for bill payment"""
        try:
            data = request.get_json()
            
            # Extract required fields
            product_code = data.get('productCode')
            customer_id = data.get('customerId')
            
            print(f'🔍 Validating bill account - Product: {product_code}, Customer: {customer_id}')
            
            # Validate required fields
            if not product_code or not customer_id:
                print('❌ Missing required fields for validation')
                return jsonify({
                    'success': False,
                    'message': 'Product code and customer ID are required',
                    'errors': {
                        'productCode': ['Product code is required'] if not product_code else [],
                        'customerId': ['Customer ID is required'] if not customer_id else []
                    }
                }), 400
            
            access_token = get_monnify_access_token()
            response = call_monnify_bills_api(
                'validate-customer',
                'POST',
                {
                    'productCode': product_code,
                    'customerId': customer_id
                },
                access_token=access_token
            )
            
            print(f'📡 Monnify validation response: {response}')
            
            validation_data = response['responseBody']
            vend_instruction = validation_data.get('vendInstruction', {})
            
            result = {
                'customerName': validation_data.get('customerName', ''),
                'priceType': validation_data.get('priceType', 'OPEN'),
                'requireValidationRef': vend_instruction.get('requireValidationRef', False),
                'validationReference': validation_data.get('validationReference'),
                'productCode': product_code,
                'customerId': customer_id
            }
            
            print(f'✅ Account validation successful for {customer_id}')
            
            return jsonify({
                'success': True,
                'data': result,
                'message': 'Account validated successfully',
                'source': 'monnify_bills'
            }), 200
            
        except Exception as e:
            print(f'❌ Account validation failed: {str(e)}')
            
            # Handle specific validation errors
            error_message = str(e)
            if 'invalid customer' in error_message.lower():
                return jsonify({
                    'success': False,
                    'message': 'Invalid customer ID. Please check the account number and try again.',
                    'errors': {'customerId': ['Invalid customer ID']},
                    'user_message': {
                        'title': 'Invalid Account',
                        'message': 'The account number you entered is not valid. Please check and try again.',
                        'type': 'validation_error'
                    }
                }), 400
            elif 'product not found' in error_message.lower():
                return jsonify({
                    'success': False,
                    'message': 'Product not found. Please select a valid product.',
                    'errors': {'productCode': ['Product not found']},
                    'user_message': {
                        'title': 'Product Not Found',
                        'message': 'The selected product is not available. Please choose another option.',
                        'type': 'validation_error'
                    }
                }), 400
            else:
                return jsonify({
                    'success': False,
                    'message': f'Validation failed: {error_message}',
                    'errors': {'general': [error_message]},
                    'user_message': {
                        'title': 'Validation Failed',
                        'message': 'Unable to validate the account. Please try again later.',
                        'type': 'service_error'
                    }
                }), 400

    @vas_bp.route('/bills/buy', methods=['POST'])
    @token_required
    def buy_bill(current_user):
        """Purchase bill payment using Monnify Bills API"""
        try:
            data = request.get_json()
            
            # Extract required fields
            category = data.get('category')
            provider = data.get('provider')
            account_number = data.get('accountNumber')
            customer_name = data.get('customerName', '')
            amount = float(data.get('amount', 0))
            product_code = data.get('productCode')
            product_name = data.get('productName', '')
            validation_reference = data.get('validationReference')
            
            print(f'🔍 Processing bill purchase:')
            print(f'   Category: {category}')
            print(f'   Provider: {provider}')
            print(f'   Account: {account_number}')
            print(f'   Amount: ₦{amount:,.2f}')
            print(f'   Product: {product_code}')
            
            # Validate required fields
            required_fields = ['category', 'provider', 'accountNumber', 'amount', 'productCode']
            missing_fields = []
            for field in required_fields:
                if not data.get(field):
                    missing_fields.append(field)
            
            if missing_fields:
                print(f'❌ Missing required fields: {missing_fields}')
                return jsonify({
                    'success': False,
                    'message': 'Missing required fields',
                    'errors': {field: [f'{field} is required'] for field in missing_fields}
                }), 400
            
            # Validate amount
            if amount <= 0:
                print(f'❌ Invalid amount: {amount}')
                return jsonify({
                    'success': False,
                    'message': 'Amount must be greater than zero',
                    'errors': {'amount': ['Amount must be greater than zero']}
                }), 400
            
            # Check wallet balance
            wallet = mongo.db.vas_wallets.find_one({'userId': current_user['_id']})
            if not wallet:
                print('❌ Wallet not found')
                return jsonify({
                    'success': False,
                    'message': 'Wallet not found. Please create a wallet first.',
                    'errors': {'wallet': ['Wallet not found']}
                }), 404
            
            if wallet['balance'] < amount:
                print(f'❌ Insufficient balance: ₦{wallet["balance"]:,.2f} < ₦{amount:,.2f}')
                return jsonify({
                    'success': False,
                    'message': 'Insufficient wallet balance',
                    'errors': {'balance': ['Insufficient wallet balance']},
                    'user_message': {
                        'title': 'Insufficient Balance',
                        'message': f'You need ₦{amount:,.2f} but only have ₦{wallet["balance"]:,.2f} in your wallet.',
                        'type': 'insufficient_balance'
                    }
                }), 402
            
            # Generate unique transaction reference
            transaction_ref = f"BILL_{uuid.uuid4().hex[:12].upper()}"
            print(f'📝 Generated transaction reference: {transaction_ref}')
            
            # Call Monnify Bills API
            access_token = get_monnify_access_token()
            
            vend_data = {
                'productCode': product_code,
                'customerId': account_number,
                'amount': amount,
                'emailAddress': current_user.get('email', ''),
                'phoneNumber': current_user.get('phoneNumber', ''),
                'reference': transaction_ref
            }
            
            # Add validation reference if required
            if validation_reference:
                vend_data['validationReference'] = validation_reference
                print(f'📝 Using validation reference: {validation_reference}')
            
            print(f'📡 Calling Monnify vend API with data: {vend_data}')
            
            response = call_monnify_bills_api(
                'vend',
                'POST',
                vend_data,
                access_token=access_token
            )
            
            print(f'📡 Monnify vend response: {response}')
            
            vend_result = response['responseBody']
            
            # Handle IN_PROGRESS status with requery
            if vend_result.get('vendStatus') == 'IN_PROGRESS':
                print('⏳ Transaction in progress, waiting 3 seconds before requery...')
                import time
                time.sleep(3)
                
                requery_response = call_monnify_bills_api(
                    f'requery?reference={transaction_ref}',
                    'GET',
                    access_token=access_token
                )
                
                print(f'📡 Monnify requery response: {requery_response}')
                vend_result = requery_response['responseBody']
            
            # Determine final status
            final_status = vend_result.get('vendStatus', 'PENDING')
            print(f'📊 Final transaction status: {final_status}')
            
            # Create transaction record
            transaction = {
                'userId': current_user['_id'],
                'type': 'BILL',
                'subtype': category.upper(),
                'billCategory': category,
                'billProvider': provider,
                'accountNumber': account_number,
                'customerName': customer_name,
                'amount': amount,
                'status': final_status,
                'transactionReference': vend_result.get('transactionReference'),
                'vendReference': vend_result.get('vendReference'),
                'description': f"Bill payment: {provider} - {account_number}",
                'provider': 'monnify',
                'createdAt': datetime.utcnow(),
                'productCode': product_code,
                'productName': vend_result.get('productName', product_name),
                'billerCode': vend_result.get('billerCode'),
                'billerName': vend_result.get('billerName'),
                'commission': vend_result.get('commission', 0),
                'payableAmount': vend_result.get('payableAmount', amount),
                'vendAmount': vend_result.get('vendAmount', amount)
            }
            
            # Insert transaction
            result = mongo.db.vas_transactions.insert_one(transaction)
            transaction['_id'] = result.inserted_id
            
            print(f'💾 Transaction saved with ID: {transaction["_id"]}')
            
            # Update wallet balance if successful
            if final_status == 'SUCCESS':
                print(f'✅ Transaction successful, deducting ₦{amount:,.2f} from wallet')
                mongo.db.vas_wallets.update_one(
                    {'userId': current_user['_id']},
                    {
                        '$inc': {'balance': -amount},
                        '$set': {'lastUpdated': datetime.utcnow()}
                    }
                )
                
                # Auto-create expense entry (auto-bookkeeping) for bill payments
                try:
                    # Generate category-specific description
                    category_display = {
                        'electricity': 'Electricity Bill',
                        'cable_tv': 'Cable TV Subscription', 
                        'internet': 'Internet Subscription',
                        'transportation': 'Transportation Payment'
                    }.get(category.lower(), 'Bill Payment')
                    
                    base_description = f'{category_display} - {provider} ₦{amount:,.2f}'
                    
                    # Generate retention-focused description
                    retention_description = generate_retention_description(
                        base_description,
                        '',  # No savings message for bills yet
                        0    # No discount applied for bills yet
                    )
                    
                    expense_entry = {
                        '_id': ObjectId(),
                        'userId': ObjectId(current_user['_id']),
                        'title': category_display,
                        'amount': amount,
                        'category': 'Utilities',  # All bill payments go under Utilities
                        'date': datetime.utcnow(),
                        'description': retention_description,
                        'isPending': False,
                        'isRecurring': False,
                        'metadata': {
                            'source': 'vas_bill_payment',
                            'billCategory': category,
                            'provider': provider,
                            'accountNumber': account_number,
                            'transactionId': str(transaction['_id']),
                            'automated': True,
                            'retentionData': {
                                'originalPrice': amount,
                                'finalPrice': amount,
                                'totalSaved': 0,
                                'userTier': 'basic'
                            }
                        },
                        'createdAt': datetime.utcnow(),
                        'updatedAt': datetime.utcnow()
                    }
                    
                    mongo.db.expenses.insert_one(expense_entry)
                    print(f'✅ Auto-created expense entry for {category_display}: ₦{amount:,.2f}')
                    
                except Exception as e:
                    print(f'⚠️ Failed to create automated expense entry: {str(e)}')
                    # Don't fail the transaction if expense entry creation fails
                
                # Create success notification
                try:
                    create_user_notification(
                        mongo,
                        current_user['_id'],
                        'Bill Payment Successful',
                        f'Your {provider} bill payment of ₦{amount:,.2f} was successful.',
                        'success',
                        {
                            'type': 'bill_payment',
                            'category': category,
                            'provider': provider,
                            'amount': amount,
                            'transactionId': str(transaction['_id'])
                        }
                    )
                except Exception as e:
                    print(f'⚠️ Failed to create notification: {str(e)}')
                
                print(f'🎉 Bill payment completed successfully!')
                
                return jsonify({
                    'success': True,
                    'data': serialize_doc(transaction),
                    'message': 'Bill payment processed successfully',
                    'user_message': {
                        'title': 'Payment Successful',
                        'message': f'Your {provider} bill payment of ₦{amount:,.2f} was successful.',
                        'type': 'success'
                    }
                }), 200
                
            elif final_status == 'FAILED':
                print(f'❌ Transaction failed')
                return jsonify({
                    'success': False,
                    'data': serialize_doc(transaction),
                    'message': 'Bill payment failed',
                    'user_message': {
                        'title': 'Payment Failed',
                        'message': f'Your {provider} bill payment could not be completed. Your wallet was not charged.',
                        'type': 'transaction_failed'
                    }
                }), 400
                
            else:  # PENDING or other status
                print(f'⏳ Transaction pending with status: {final_status}')
                return jsonify({
                    'success': True,
                    'data': serialize_doc(transaction),
                    'message': 'Bill payment is being processed',
                    'user_message': {
                        'title': 'Payment Processing',
                        'message': f'Your {provider} bill payment is being processed. You will be notified once completed.',
                        'type': 'pending'
                    }
                }), 200
            
        except Exception as e:
            print(f'❌ Bill payment failed with error: {str(e)}')
            
            # Handle specific errors
            error_message = str(e)
            if 'insufficient balance' in error_message.lower():
                return jsonify({
                    'success': False,
                    'message': 'Insufficient wallet balance',
                    'errors': {'balance': ['Insufficient wallet balance']},
                    'user_message': {
                        'title': 'Insufficient Balance',
                        'message': 'You don\'t have enough funds in your wallet to complete this transaction.',
                        'type': 'insufficient_balance'
                    }
                }), 402
            elif 'timeout' in error_message.lower():
                return jsonify({
                    'success': False,
                    'message': 'Transaction timeout',
                    'errors': {'timeout': ['Transaction timed out']},
                    'user_message': {
                        'title': 'Transaction Timeout',
                        'message': 'The transaction is taking longer than expected. Please try again.',
                        'type': 'timeout'
                    }
                }), 408
            else:
                return jsonify({
                    'success': False,
                    'message': f'Bill payment failed: {error_message}',
                    'errors': {'general': [error_message]},
                    'user_message': {
                        'title': 'Payment Failed',
                        'message': 'Unable to process your bill payment. Please try again later.',
                        'type': 'service_error'
                    }
                }), 500

    return vas_bp

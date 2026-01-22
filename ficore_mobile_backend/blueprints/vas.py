"""
VAS (Value Added Services) Blueprint - Production Grade
Handles wallet management and utility purchases (Airtime, Data, etc.)

Security: API keys in environment variables, idempotency protection, webhook verification
Providers: Monnify (wallet), Peyflex (primary VAS), VTpass (backup)
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
                    access_token = data['responseBody']['accessToken']
                    print(f'✅ Monnify access token obtained: {access_token[:20]}...')
                    return access_token
                else:
                    raise Exception(f"Monnify auth failed: {data.get('responseMessage', 'Unknown error')}")
            else:
                raise Exception(f"Monnify auth HTTP error: {response.status_code} - {response.text}")
                
        except Exception as e:
            print(f'❌ Failed to get Monnify access token: {str(e)}')
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
                response = requests.get(url, headers=headers, timeout=30)
            elif method.upper() == 'POST':
                response = requests.post(url, headers=headers, json=data, timeout=30)
            else:
                raise Exception(f"Unsupported HTTP method: {method}")
            
            print(f'📡 Monnify Bills API {method} {endpoint}: {response.status_code}')
            
            if response.status_code == 200:
                return response.json()
            else:
                print(f'❌ Monnify Bills API error: {response.status_code} - {response.text}')
                raise Exception(f'Monnify Bills API error: {response.status_code} - {response.text}')
                
        except Exception as e:
            print(f'❌ Monnify Bills API call failed: {str(e)}')
            raise Exception(f'Monnify Bills API failed: {str(e)}')
    
    def generate_retention_description(base_description, savings_message, discount_applied):
        """Generate retention-focused transaction description"""
        try:
            if discount_applied > 0:
                return f"{base_description} (Saved ₦{discount_applied:.0f})"
            else:
                return base_description
        except Exception as e:
            print(f'⚠️ Error generating retention description: {str(e)}')
            return base_description  # Fallback to base description
    

    # ==================== PRICING ENDPOINTS ====================
    
    @vas_bp.route('/pricing/calculate', methods=['POST'])
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
            print(f'❌ Error calculating pricing: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to calculate pricing',
                'errors': {'general': [str(e)]}
            }), 500
    
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
                print(f'📡 Calling Peyflex airtime networks API: {url}')
                
                response = requests.get(url, timeout=30)
                print(f'📥 Peyflex airtime networks response status: {response.status_code}')
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        print(f'📊 Peyflex airtime response: {data}')
                        
                        # Handle different response formats
                        networks_list = []
                        if isinstance(data, dict) and 'networks' in data:
                            networks_list = data['networks']
                        elif isinstance(data, list):
                            networks_list = data
                        else:
                            print('⚠️ Unexpected airtime networks response format')
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
                        
                        print(f'✅ Successfully transformed {len(transformed_networks)} airtime networks from Peyflex')
                        return jsonify({
                            'success': True,
                            'data': transformed_networks,
                            'message': 'Airtime networks retrieved from Peyflex (fallback)',
                            'source': 'peyflex_fallback'
                        }), 200
                        
                    except Exception as json_error:
                        print(f'❌ Error parsing Peyflex airtime networks response: {json_error}')
                        raise Exception(f'Invalid airtime networks response from Peyflex: {json_error}')
                
                else:
                    print(f'🚨 Peyflex airtime networks API error: {response.status_code} - {response.text}')
                    raise Exception(f'Peyflex airtime networks API returned {response.status_code}')
            
        except Exception as e:
            print(f'❌ Error getting airtime networks from both providers: {str(e)}')
            
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

# ==================== HELPER FUNCTIONS ====================
    
    def generate_request_id(user_id, transaction_type):
        """Generate unique request ID for idempotency"""
        timestamp = int(datetime.utcnow().timestamp())
        unique_suffix = str(uuid.uuid4())[:8]
        return f'FICORE_{transaction_type}_{user_id}_{timestamp}_{unique_suffix}'
    
    def check_eligibility(user_id):
        """
        Check if user is eligible for dedicated account (Path B)
        User must meet ONE of these criteria:
        1. Used app for 3+ consecutive days
        2. Recorded 10+ transactions (income/expense)
        """
        user = mongo.db.users.find_one({'_id': ObjectId(user_id)})
        
        # Check 1: Consecutive days - Use rewards.streak as authoritative source
        rewards_record = mongo.db.rewards.find_one({'user_id': ObjectId(user_id)})
        login_streak = rewards_record.get('streak', 0) if rewards_record else 0
        if login_streak >= 3:
            return True, "3-day streak"
        
        # Check 2: Total transactions
        total_txns = mongo.db.income.count_documents({'userId': ObjectId(user_id)})
        total_txns += mongo.db.expenses.count_documents({'userId': ObjectId(user_id)})
        if total_txns >= 10:
            return True, "10+ transactions"
        
        return False, None
    
    def get_eligibility_progress(user_id):
        """Get user's progress towards eligibility"""
        user = mongo.db.users.find_one({'_id': ObjectId(user_id)})
        
        # Use rewards.streak as authoritative source for login streak
        rewards_record = mongo.db.rewards.find_one({'user_id': ObjectId(user_id)})
        login_streak = rewards_record.get('streak', 0) if rewards_record else 0
        total_txns = mongo.db.income.count_documents({'userId': ObjectId(user_id)})
        total_txns += mongo.db.expenses.count_documents({'userId': ObjectId(user_id)})
        
        # Return flat structure that matches frontend expectations
        return {
            'loginDays': login_streak,  # Frontend expects 'loginDays', not 'loginStreak'
            'totalTransactions': total_txns
        }
    
    def call_monnify_auth():
        """Get Monnify authentication token"""
        try:
            auth_response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v1/auth/login',
                auth=(MONNIFY_API_KEY, MONNIFY_SECRET_KEY),
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if auth_response.status_code != 200:
                raise Exception(f'Monnify auth failed: {auth_response.text}')
            
            return auth_response.json()['responseBody']['accessToken']
        except Exception as e:
            print(f'❌ Monnify auth error: {str(e)}')
            raise
    
    def call_monnify_bvn_verification(bvn, name, dob, mobile):
        """
        Call Monnify BVN verification API
        Cost: ₦10 per successful request
        """
        try:
            access_token = call_monnify_auth()
            
            response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v1/vas/bvn-details-match',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                },
                json={
                    'bvn': bvn,
                    'name': name,
                    'dateOfBirth': dob,
                    'mobileNo': mobile
                },
                timeout=30
            )
            
            if response.status_code != 200:
                raise Exception(f'BVN verification failed: {response.text}')
            
            data = response.json()
            if not data.get('requestSuccessful'):
                raise Exception(f'BVN verification failed: {data.get("responseMessage")}')
            
            return data['responseBody']
        except Exception as e:
            print(f'❌ BVN verification error: {str(e)}')
            raise
    
    def call_monnify_nin_verification(nin):
        """
        Call Monnify NIN verification API
        Cost: ₦60 per successful request
        Returns NIN holder's details for validation
        """
        try:
            access_token = call_monnify_auth()
            
            response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v1/vas/nin-details',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                },
                json={'nin': nin},
                timeout=30
            )
            
            if response.status_code != 200:
                raise Exception(f'NIN verification failed: {response.text}')
            
            data = response.json()
            if not data.get('requestSuccessful'):
                raise Exception(f'NIN verification failed: {data.get("responseMessage")}')
            
            return data['responseBody']
        except Exception as e:
            print(f'❌ NIN verification error: {str(e)}')
            raise
    
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
    
    
    def call_peyflex_airtime(network, amount, phone_number, request_id):
        """Call Peyflex Airtime API with exact format from documentation"""
        # Use the exact format from Peyflex documentation
        payload = {
            'network': network.lower(),  # Documentation shows lowercase: "mtn"
            'amount': int(amount),
            'mobile_number': phone_number
            # NOTE: Do NOT send request_id - not shown in documentation example
        }
        
        print(f'📤 Peyflex airtime purchase payload: {payload}')
        print(f'📤 Using API token: {PEYFLEX_API_TOKEN[:10]}...{PEYFLEX_API_TOKEN[-4:]}')
        
        headers = {
            'Authorization': f'Token {PEYFLEX_API_TOKEN}',  # Documentation shows "Token" not "Bearer"
            'Content-Type': 'application/json',
            'User-Agent': 'FiCore-Backend/1.0'
        }
        
        url = f'{PEYFLEX_BASE_URL}/api/airtime/topup/'
        print(f'📡 Calling Peyflex airtime API: {url}')
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            print(f'📥 Peyflex airtime response: {response.status_code}')
            print(f'📥 Response body: {response.text[:500]}')
            
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as json_error:
                    print(f'❌ Error parsing Peyflex airtime response: {json_error}')
                    raise Exception(f'Invalid response format from Peyflex: {json_error}')
            elif response.status_code == 400:
                print('🚨 Peyflex airtime API returned 400 Bad Request')
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', response.text)
                except:
                    error_msg = response.text
                raise Exception(f'Invalid airtime request: {error_msg}')
            elif response.status_code == 403:
                print('🚨 Peyflex airtime API returned 403 Forbidden')
                print('🔍 This usually means: API token invalid, account not activated, or IP not whitelisted')
                raise Exception('Airtime service access denied - check API credentials and account status')
            elif response.status_code == 404:
                print('🚨 Peyflex airtime API returned 404 Not Found')
                raise Exception('Airtime endpoint not found - check API URL')
            else:
                print(f'🚨 Peyflex airtime API error: {response.status_code} - {response.text}')
                raise Exception(f'Peyflex airtime API error: {response.status_code} - {response.text}')
                
        except requests.exceptions.ConnectionError as e:
            print(f'❌ Connection error to Peyflex: {str(e)}')
            raise Exception('Unable to connect to Peyflex servers - check network connectivity')
        except requests.exceptions.Timeout as e:
            print(f'❌ Timeout error to Peyflex: {str(e)}')
            raise Exception('Peyflex API request timed out - try again later')
        except Exception as e:
            if 'Invalid response format' in str(e) or 'Invalid airtime request' in str(e) or 'access denied' in str(e):
                raise  # Re-raise our custom exceptions
            print(f'❌ Unexpected error calling Peyflex: {str(e)}')
            raise Exception(f'Unexpected error with Peyflex API: {str(e)}')

    # ==================== MONNIFY BILLS API FUNCTIONS ====================
    
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
            
            print(f'📱 Using Monnify product: {airtime_product["name"]} (Code: {airtime_product["code"]})')
            
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
            
            print(f'✅ Monnify customer validation successful for {phone_number}')
            
            # Step 6: Prepare vend request
            vend_data = {
                'productCode': airtime_product['code'],
                'customerId': phone_number,
                'amount': int(amount),
                'reference': request_id,  # Use our unique request ID
                'phoneNumber': phone_number,
                'emailAddress': 'customer@ficoreafrica.com'  # Default email
            }
            
            # Check if validation reference is required
            vend_instruction = validation_response['responseBody'].get('vendInstruction', {})
            if vend_instruction.get('requireValidationRef', False):
                validation_ref = validation_response['responseBody'].get('validationReference')
                if validation_ref:
                    vend_data['validationReference'] = validation_ref
                    print(f'🔐 Using validation reference: {validation_ref}')
            
            # Step 7: Execute vend (purchase)
            vend_response = call_monnify_bills_api(
                'vend',
                'POST', 
                vend_data,
                access_token=access_token
            )
            
            vend_result = vend_response['responseBody']
            
            if vend_result.get('vendStatus') == 'SUCCESS':
                print(f'✅ Monnify airtime purchase successful: {vend_result["transactionReference"]}')
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
                print(f'⏳ Monnify transaction in progress, checking status...')
                import time
                time.sleep(3)  # Wait 3 seconds
                
                requery_response = call_monnify_bills_api(
                    f'requery?reference={request_id}',
                    'GET',
                    access_token=access_token
                )
                
                final_result = requery_response['responseBody']
                if final_result.get('vendStatus') == 'SUCCESS':
                    print(f'✅ Monnify airtime purchase completed: {final_result["transactionReference"]}')
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
            print(f'❌ Monnify airtime purchase failed: {str(e)}')
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
                    print(f'⚠️ Using fallback data product: {data_product["name"]}')
                else:
                    raise Exception(f'No data products found for {network}')
            
            print(f'📊 Using Monnify data product: {data_product["name"]} (Code: {data_product["code"]})')
            
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
            
            print(f'✅ Monnify data customer validation successful for {phone_number}')
            
            # Step 6: Prepare vend request
            vend_amount = data_product.get('price', 0)
            if not vend_amount or vend_amount <= 0:
                raise Exception(f'Invalid data product price: {vend_amount}')
            
            vend_data = {
                'productCode': data_product['code'],
                'customerId': phone_number,
                'amount': vend_amount,
                'reference': request_id,
                'phoneNumber': phone_number,
                'emailAddress': 'customer@ficoreafrica.com'
            }
            
            # Check validation reference requirement
            vend_instruction = validation_response['responseBody'].get('vendInstruction', {})
            if vend_instruction.get('requireValidationRef', False):
                validation_ref = validation_response['responseBody'].get('validationReference')
                if validation_ref:
                    vend_data['validationReference'] = validation_ref
                    print(f'🔐 Using validation reference for data: {validation_ref}')
            
            # Step 7: Execute vend
            vend_response = call_monnify_bills_api(
                'vend',
                'POST',
                vend_data,
                access_token=access_token
            )
            
            vend_result = vend_response['responseBody']
            
            if vend_result.get('vendStatus') == 'SUCCESS':
                print(f'✅ Monnify data purchase successful: {vend_result["transactionReference"]}')
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
                print(f'⏳ Monnify data transaction in progress, checking status...')
                import time
                time.sleep(3)
                
                requery_response = call_monnify_bills_api(
                    f'requery?reference={request_id}',
                    'GET',
                    access_token=access_token
                )
                
                final_result = requery_response['responseBody']
                if final_result.get('vendStatus') == 'SUCCESS':
                    print(f'✅ Monnify data purchase completed: {final_result["transactionReference"]}')
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
            print(f'❌ Monnify data purchase failed: {str(e)}')
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
        
        print(f'📤 Peyflex airtime purchase payload: {payload}')
        print(f'📤 Using API token: {PEYFLEX_API_TOKEN[:10]}...{PEYFLEX_API_TOKEN[-4:]}')
        
        headers = {
            'Authorization': f'Token {PEYFLEX_API_TOKEN}',  # Documentation shows "Token" not "Bearer"
            'Content-Type': 'application/json',
            'User-Agent': 'FiCore-Backend/1.0'
        }
        
        url = f'{PEYFLEX_BASE_URL}/api/airtime/topup/'
        print(f'📡 Calling Peyflex airtime API: {url}')
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            print(f'📥 Peyflex airtime response: {response.status_code}')
            print(f'📥 Response body: {response.text[:500]}')
            
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as json_error:
                    print(f'❌ Error parsing Peyflex airtime response: {json_error}')
                    raise Exception(f'Invalid response format from Peyflex: {json_error}')
            elif response.status_code == 400:
                print('🚨 Peyflex airtime API returned 400 Bad Request')
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', response.text)
                except:
                    error_msg = response.text
                raise Exception(f'Invalid airtime request: {error_msg}')
            elif response.status_code == 403:
                print('🚨 Peyflex airtime API returned 403 Forbidden')
                print('🔍 This usually means: API token invalid, account not activated, or IP not whitelisted')
                raise Exception('Airtime service access denied - check API credentials and account status')
            elif response.status_code == 404:
                print('🚨 Peyflex airtime API returned 404 Not Found')
                raise Exception('Airtime endpoint not found - check API URL')
            else:
                print(f'🚨 Peyflex airtime API error: {response.status_code} - {response.text}')
                raise Exception(f'Peyflex airtime API error: {response.status_code} - {response.text}')
                
        except requests.exceptions.ConnectionError as e:
            print(f'❌ Connection error to Peyflex: {str(e)}')
            raise Exception('Unable to connect to Peyflex servers - check network connectivity')
        except requests.exceptions.Timeout as e:
            print(f'❌ Timeout error to Peyflex: {str(e)}')
            raise Exception('Peyflex API request timed out - try again later')
        except Exception as e:
            if 'Invalid response format' in str(e) or 'Invalid airtime request' in str(e) or 'access denied' in str(e):
                raise  # Re-raise our custom exceptions
            print(f'❌ Unexpected error calling Peyflex: {str(e)}')
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
        
        print(f'📤 Peyflex data purchase payload: {payload}')
        print(f'📤 Using API token: {PEYFLEX_API_TOKEN[:10]}...{PEYFLEX_API_TOKEN[-4:]}')
        
        headers = {
            'Authorization': f'Token {PEYFLEX_API_TOKEN}',  # Documentation shows "Token" not "Bearer"
            'Content-Type': 'application/json',
            'User-Agent': 'FiCore-Backend/1.0'
        }
        
        url = f'{PEYFLEX_BASE_URL}/api/data/purchase/'
        print(f'📡 Calling Peyflex data purchase API: {url}')
        
        try:
            response = requests.post(
                url,
                headers=headers,
                json=payload,
                timeout=30
            )
            
            print(f'📥 Peyflex data purchase response: {response.status_code}')
            print(f'📥 Response body: {response.text[:500]}')
            
            if response.status_code == 200:
                try:
                    return response.json()
                except Exception as json_error:
                    print(f'❌ Error parsing Peyflex data purchase response: {json_error}')
                    raise Exception(f'Invalid response format from Peyflex: {json_error}')
            elif response.status_code == 400:
                print('🚨 Peyflex data purchase API returned 400 Bad Request')
                try:
                    error_data = response.json()
                    error_msg = error_data.get('message', response.text)
                except:
                    error_msg = response.text
                raise Exception(f'Invalid data purchase request: {error_msg}')
            elif response.status_code == 403:
                print('🚨 Peyflex data purchase API returned 403 Forbidden')
                print('🔍 This usually means: API token invalid, account not activated, or IP not whitelisted')
                raise Exception('Data purchase service access denied - check API credentials and account status')
            elif response.status_code == 404:
                print('🚨 Peyflex data purchase API returned 404 Not Found')
                raise Exception('Data purchase endpoint not found - check API URL')
            else:
                print(f'🚨 Peyflex data purchase API error: {response.status_code} - {response.text}')
                raise Exception(f'Peyflex data purchase API error: {response.status_code} - {response.text}')
                
        except requests.exceptions.ConnectionError as e:
            print(f'❌ Connection error to Peyflex: {str(e)}')
            raise Exception('Unable to connect to Peyflex servers - check network connectivity')
        except requests.exceptions.Timeout as e:
            print(f'❌ Timeout error to Peyflex: {str(e)}')
            raise Exception('Peyflex API request timed out - try again later')
        except Exception as e:
            if 'Invalid response format' in str(e) or 'Invalid data purchase request' in str(e) or 'access denied' in str(e):
                raise  # Re-raise our custom exceptions
            print(f'❌ Unexpected error calling Peyflex: {str(e)}')
            raise Exception(f'Unexpected error with Peyflex API: {str(e)}')

    # ==================== NETWORK AND PLANS ENDPOINTS ====================
    
    @vas_bp.route('/networks/airtime', methods=['GET'])
    @token_required
    def create_wallet(current_user):
        """Create virtual account number (VAN) for user via Monnify"""
        try:
            user_id = str(current_user['_id'])
            
            existing_wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if existing_wallet:
                return jsonify({
                    'success': True,
                    'data': serialize_doc(existing_wallet),
                    'message': 'Wallet already exists'
                }), 200
            
            auth_response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v1/auth/login',
                auth=(MONNIFY_API_KEY, MONNIFY_SECRET_KEY),
                headers={'Content-Type': 'application/json'},
                timeout=30
            )
            
            if auth_response.status_code != 200:
                raise Exception(f'Monnify auth failed: {auth_response.text}')
            
            access_token = auth_response.json()['responseBody']['accessToken']
            
            account_data = {
                'accountReference': f'FICORE_{user_id}',
                'accountName': f"{current_user.get('firstName', '')} {current_user.get('lastName', '')}".strip()[:50],  # Monnify 50-char limit
                'currencyCode': 'NGN',
                'contractCode': MONNIFY_CONTRACT_CODE,
                'customerEmail': current_user.get('email', ''),
                'customerName': f"{current_user.get('firstName', '')} {current_user.get('lastName', '')}".strip()[:50],  # Monnify 50-char limit
                'getAllAvailableBanks': True
            }
            
            van_response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v2/bank-transfer/reserved-accounts',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                },
                json=account_data,
                timeout=30
            )
            
            if van_response.status_code != 200:
                raise Exception(f'VAN creation failed: {van_response.text}')
            
            van_data = van_response.json()['responseBody']
            
            wallet = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'balance': 0.0,
                'accountReference': van_data['accountReference'],
                'accountName': van_data['accountName'],
                'accounts': van_data['accounts'],
                'status': 'active',
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.vas_wallets.insert_one(wallet)
            
            return jsonify({
                'success': True,
                'data': serialize_doc(wallet),
                'message': 'Wallet created successfully'
            }), 201
            
        except Exception as e:
            print(f'❌ Error creating wallet: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to create wallet',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/wallet/balance', methods=['GET'])
    @token_required
    def get_wallet_balance(current_user):
        """Get user's wallet balance"""
        try:
            user_id = str(current_user['_id'])
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            
            if not wallet:
                return jsonify({
                    'success': False,
                    'message': 'Wallet not found. Please create a wallet first.'
                }), 404
            
            return jsonify({
                'success': True,
                'data': {
                    'balance': wallet.get('balance', 0.0),
                    'accounts': wallet.get('accounts', []),
                    'accountName': wallet.get('accountName', ''),
                    'status': wallet.get('status', 'active')
                },
                'message': 'Wallet balance retrieved successfully'
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting wallet balance: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve wallet balance',
                'errors': {'general': [str(e)]}
            }), 500
    
    # ==================== ELIGIBILITY & KYC ENDPOINTS ====================
    
    @vas_bp.route('/check-eligibility', methods=['GET'])
    @token_required
    def check_eligibility_endpoint(current_user):
        """Check if user is eligible for dedicated account (Path B)"""
        try:
            user_id = str(current_user['_id'])
            eligible, reason = check_eligibility(user_id)
            progress = get_eligibility_progress(user_id)
            
            return jsonify({
                'success': True,
                'data': {
                    'eligible': eligible,
                    'reason': reason,
                    'progress': progress
                },
                'message': 'Eligibility checked successfully'
            }), 200
            
        except Exception as e:
            print(f'❌ Error checking eligibility: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to check eligibility',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/check-existing-bvn-nin', methods=['POST'])
    @token_required
    def check_existing_bvn_nin(current_user):
        """
        Check if BVN or NIN already exists in the database
        """
        try:
            data = request.json
            bvn = data.get('bvn', '').strip()
            nin = data.get('nin', '').strip()
            
            # Validate input
            if len(bvn) != 11 or not bvn.isdigit():
                return jsonify({
                    'success': False,
                    'exists': False,
                    'message': 'Invalid BVN format'
                }), 400
            
            if len(nin) != 11 or not nin.isdigit():
                return jsonify({
                    'success': False,
                    'exists': False,
                    'message': 'Invalid NIN format'
                }), 400
            
            # Check if BVN exists in any wallet
            bvn_exists = mongo.db.vas_wallets.find_one({
                'bvn': bvn,
                'status': 'ACTIVE'
            })
            
            # Check if NIN exists in any wallet
            nin_exists = mongo.db.vas_wallets.find_one({
                'nin': nin,
                'status': 'ACTIVE'
            })
            
            # Also check in user profiles
            bvn_in_profile = mongo.db.users.find_one({
                'bvn': bvn
            })
            
            nin_in_profile = mongo.db.users.find_one({
                'nin': nin
            })
            
            exists = bool(bvn_exists or nin_exists or bvn_in_profile or nin_in_profile)
            
            if exists:
                message = 'This BVN or NIN has already been used for account creation.'
            else:
                message = 'BVN and NIN are available for use.'
            
            return jsonify({
                'success': True,
                'exists': exists,
                'message': message,
                'details': {
                    'bvn_in_wallet': bool(bvn_exists),
                    'nin_in_wallet': bool(nin_exists),
                    'bvn_in_profile': bool(bvn_in_profile),
                    'nin_in_profile': bool(nin_in_profile)
                }
            }), 200
            
        except Exception as e:
            print(f'ERROR: Error checking existing BVN/NIN: {str(e)}')
            return jsonify({
                'success': False,
                'exists': False,
                'message': 'Error checking records',
                'error': str(e)
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
            access_token = call_monnify_auth()
            
            account_data = {
                'accountReference': f'FICORE_{user_id}',
                'accountName': user_name[:50],  # Monnify 50-char limit
                'currencyCode': 'NGN',
                'contractCode': MONNIFY_CONTRACT_CODE,
                'customerEmail': user_email,
                'customerName': user_name[:50],  # Monnify 50-char limit
                'bvn': bvn,
                'nin': nin,
                'getAllAvailableBanks': True  # Get all available banks for user choice
            }
            
            print(f"🏦 Creating Monnify reserved account with BVN: {bvn[:3]}***{bvn[-3:]}")
            
            van_response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v2/bank-transfer/reserved-accounts',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                },
                json=account_data,
                timeout=30
            )
            
            if van_response.status_code != 200:
                print(f"❌ Monnify account creation failed: {van_response.status_code} - {van_response.text}")
                raise Exception(f'Reserved account creation failed: {van_response.text}')
            
            van_data = van_response.json()['responseBody']
            print(f"✅ Monnify account created successfully with {len(van_data.get('accounts', []))} banks")
            
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
                'accountReference': van_data['accountReference'],
                'contractCode': van_data['contractCode'],
                'accounts': van_data['accounts'],
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
            
            # Update user profile with provided phone number
            profile_update = {
                'phone': phone_number,  # Save phone number to profile
                'updatedAt': datetime.utcnow()
            }
            
            # Update user profile
            mongo.db.users.update_one(
                {'_id': ObjectId(user_id)},
                {'$set': profile_update}
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
                    'accounts': van_data['accounts'],  # All available bank accounts
                    'accountReference': van_data['accountReference'],
                    'contractCode': van_data['contractCode'],
                    'tier': 'TIER_2',
                    'kycVerified': True,
                    'verifiedName': user_name,
                    'createdAt': wallet_data['createdAt'].isoformat() + 'Z',
                    # Keep backward compatibility - return first account as default
                    'defaultAccount': {
                        'accountNumber': van_data['accounts'][0].get('accountNumber', '') if van_data['accounts'] else '',
                        'accountName': van_data['accounts'][0].get('accountName', '') if van_data['accounts'] else '',
                        'bankName': van_data['accounts'][0].get('bankName', 'Wema Bank') if van_data['accounts'] else 'Wema Bank',
                        'bankCode': van_data['accounts'][0].get('bankCode', '035') if van_data['accounts'] else '035',
                    }
                },
                'message': f'Account created successfully with {len(van_data["accounts"])} available banks!'
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
            access_token = call_monnify_auth()
            
            account_data = {
                'accountReference': f'FICORE_{user_id}',
                'accountName': verification['verifiedName'][:50],  # Monnify 50-char limit
                'currencyCode': 'NGN',
                'contractCode': MONNIFY_CONTRACT_CODE,
                'customerEmail': current_user.get('email', ''),
                'customerName': verification['verifiedName'][:50],  # Monnify 50-char limit
                'bvn': verification['bvn'],
                'nin': verification['nin'],  # Include NIN for full Tier 2 compliance
                'getAllAvailableBanks': True  # Moniepoint default, user choice
            }
            
            van_response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v2/bank-transfer/reserved-accounts',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                },
                json=account_data,
                timeout=30
            )
            
            if van_response.status_code != 200:
                raise Exception(f'Reserved account creation failed: {van_response.text}')
            
            van_data = van_response.json()['responseBody']
            
            # Create wallet with KYC info (BVN + NIN for full Tier 2)
            wallet = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'balance': 0.0,
                'accountReference': van_data['accountReference'],
                'accountName': van_data['accountName'],
                'accounts': van_data['accounts'],
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
                        try:
                            data = response.json()
                            print(f'📊 Peyflex response: {data}')
                            
                            # Handle the correct response format from documentation
                            networks_list = []
                            if isinstance(data, dict):
                                if 'networks' in data:
                                    networks_list = data['networks']
                                    print(f'✅ Found {len(networks_list)} networks in response.networks')
                                elif 'data' in data:
                                    networks_list = data['data']
                                    print(f'✅ Found {len(networks_list)} networks in response.data')
                                else:
                                    print(f'⚠️ Dict response without networks/data key: {list(data.keys())}')
                                    networks_list = []
                            elif isinstance(data, list):
                                networks_list = data
                                print(f'✅ Direct array with {len(networks_list)} networks')
                            else:
                                print(f'⚠️ Unexpected response format: {data}')
                                networks_list = []
                            
                            # Transform to our format
                            transformed_networks = []
                            for network in networks_list:
                                if not isinstance(network, dict):
                                    print(f'⚠️ Skipping non-dict network: {network}')
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
                                    print(f'⚠️ Skipping invalid network: {network}')
                            
                            print(f'✅ Successfully transformed {len(transformed_networks)} valid networks from Peyflex')
                            
                            if len(transformed_networks) > 0:
                                return jsonify({
                                    'success': True,
                                    'data': transformed_networks,
                                    'message': 'Data networks retrieved from Peyflex (fallback)',
                                    'source': 'peyflex_fallback'
                                }), 200
                            else:
                                print('⚠️ No valid networks found in Peyflex response')
                                # Fall through to emergency fallback
                                
                        except Exception as json_error:
                            print(f'❌ Error parsing Peyflex networks response: {json_error}')
                            print(f'📄 Raw response: {response.text}')
                            # Fall through to emergency fallback
                    
                    elif response.status_code == 403:
                        print('🚨 Peyflex networks API returned 403 Forbidden')
                        print('🔍 This usually means: API token invalid, account not activated, or IP not whitelisted')
                        # Fall through to emergency fallback
                    
                    else:
                        print(f'🚨 Peyflex networks API error: {response.status_code} - {response.text}')
                        # Fall through to emergency fallback
                        
                except requests.exceptions.ConnectionError as e:
                    print(f'❌ Connection error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
                except requests.exceptions.Timeout as e:
                    print(f'❌ Timeout error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
            
        except Exception as e:
            print(f'❌ Error getting data networks from both providers: {str(e)}')
        
        # Emergency fallback data networks
        print('🔄 Using emergency fallback data networks')
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
    
    @vas_bp.route('/data-plans/<network>', methods=['GET'])
    @token_required
    def get_data_plans(current_user, network):
        """Get data plans for a specific network from Monnify Bills API (primary) with Peyflex fallback"""
        try:
            print(f'🔍 Fetching data plans for network: {network}')
            
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
                    print(f'✅ Successfully retrieved {len(plans)} data plans from Monnify for {network}')
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
                print(f'⚠️ Monnify data plans failed for {network}: {str(monnify_error)}')
                
                # Fallback to Peyflex
                print(f'🔄 Falling back to Peyflex for {network} data plans')
                
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
                    print(f'📝 Mapped {network} to {full_network_id}')
                else:
                    full_network_id = network_lower
                    print(f'📝 Using network ID as-is: {full_network_id}')
                
                headers = {
                    'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                    'Content-Type': 'application/json',
                    'User-Agent': 'FiCore-Backend/1.0'
                }
                
                url = f'{PEYFLEX_BASE_URL}/api/data/plans/?network={full_network_id}'
                print(f'📡 Calling Peyflex plans API: {url}')
                
                try:
                    response = requests.get(url, headers=headers, timeout=30)
                    print(f'📥 Peyflex plans response status: {response.status_code}')
                    print(f'📥 Response preview: {response.text[:500]}')
                    
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            print(f'📊 Peyflex plans response type: {type(data)}')
                            
                            # Handle the correct response format from documentation
                            plans_list = []
                            if isinstance(data, dict):
                                if 'plans' in data:
                                    plans_list = data['plans']
                                    print(f'✅ Found {len(plans_list)} plans in response.plans')
                                elif 'data' in data:
                                    plans_list = data['data']
                                    print(f'✅ Found {len(plans_list)} plans in response.data')
                                else:
                                    print(f'⚠️ Dict response without plans/data key: {list(data.keys())}')
                                    # Try to use the dict itself if it looks like a plan
                                    if 'plan_code' in data or 'amount' in data:
                                        plans_list = [data]
                                    else:
                                        plans_list = []
                            elif isinstance(data, list):
                                plans_list = data
                                print(f'✅ Direct array with {len(plans_list)} plans')
                            else:
                                print(f'⚠️ Unexpected response format: {data}')
                                plans_list = []
                            
                            # Transform to our format
                            transformed_plans = []
                            for plan in plans_list:
                                if not isinstance(plan, dict):
                                    print(f'⚠️ Skipping non-dict plan: {plan}')
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
                                    print(f'⚠️ Skipping invalid plan: {plan}')
                            
                            print(f'✅ Successfully transformed {len(transformed_plans)} valid plans from Peyflex')
                            
                            if len(transformed_plans) > 0:
                                return jsonify({
                                    'success': True,
                                    'data': transformed_plans,
                                    'message': f'Data plans for {network.upper()} from Peyflex (fallback)',
                                    'source': 'peyflex_fallback',
                                    'network_id': full_network_id
                                }), 200
                            else:
                                print(f'⚠️ No valid plans found for {full_network_id}')
                                # Fall through to emergency fallback
                                
                        except Exception as json_error:
                            print(f'❌ Error parsing Peyflex plans response: {json_error}')
                            print(f'📄 Raw response: {response.text}')
                            # Fall through to emergency fallback
                    
                    elif response.status_code == 404:
                        print(f'🚨 Network {full_network_id} not found on Peyflex (404)')
                        # Fall through to emergency fallback
                    
                    elif response.status_code == 403:
                        print(f'🚨 Peyflex plans API returned 403 Forbidden')
                        print('🔍 This usually means: API token invalid, account not activated, or IP not whitelisted')
                        # Fall through to emergency fallback
                    
                    else:
                        print(f'🚨 Peyflex plans API error: {response.status_code} - {response.text}')
                        # Fall through to emergency fallback
                        
                except requests.exceptions.ConnectionError as e:
                    print(f'❌ Connection error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
                except requests.exceptions.Timeout as e:
                    print(f'❌ Timeout error to Peyflex: {str(e)}')
                    # Fall through to emergency fallback
                except Exception as e:
                    print(f'❌ Unexpected error calling Peyflex: {str(e)}')
                    # Fall through to emergency fallback
            
        except Exception as e:
            print(f'❌ Error in get_data_plans: {str(e)}')
        
        # Return emergency fallback plans
        print(f'🔄 Using emergency fallback plans for {network}')
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
    
    def get_transaction_display_info(txn):
        """Generate user-friendly description and category for VAS transactions"""
        txn_type = txn.get('type', 'UNKNOWN').upper()
        bill_category = txn.get('billCategory', '').lower()
        provider = txn.get('provider', '')
        bill_provider = txn.get('billProvider', '')
        amount = txn.get('amount', 0)
        phone_number = txn.get('phoneNumber', '')
        plan_name = txn.get('planName', '')
        account_number = txn.get('accountNumber', '')
        
        # Generate description and category based on transaction type
        if txn_type == 'AIRTIME_PURCHASE':
            description = f"Airtime purchase ₦{amount:,.2f}"
            if phone_number:
                masked_phone = phone_number[-4:] + '****' if len(phone_number) > 4 else phone_number
                description = f"Airtime ₦{amount:,.2f} sent to {masked_phone}"
            category = "Utilities"
            
        elif txn_type == 'DATA_PURCHASE':
            description = f"Data purchase ₦{amount:,.2f}"
            if plan_name and phone_number:
                masked_phone = phone_number[-4:] + '****' if len(phone_number) > 4 else phone_number
                description = f"{plan_name} for {masked_phone}"
            elif phone_number:
                masked_phone = phone_number[-4:] + '****' if len(phone_number) > 4 else phone_number
                description = f"Data ₦{amount:,.2f} for {masked_phone}"
            category = "Utilities"
            
        elif txn_type == 'WALLET_FUNDING':
            description = f"Wallet funded ₦{amount:,.2f}"
            category = "Transfer"
            
        elif txn_type == 'BILL':
            # Handle bill payments based on category
            if bill_category == 'electricity':
                description = f"Electricity bill ₦{amount:,.2f}"
                if bill_provider:
                    description = f"Electricity bill ₦{amount:,.2f} - {bill_provider}"
                category = "Utilities"
                
            elif bill_category == 'cable_tv':
                description = f"Cable TV subscription ₦{amount:,.2f}"
                if bill_provider:
                    description = f"Cable TV ₦{amount:,.2f} - {bill_provider}"
                category = "Entertainment"
                
            elif bill_category == 'internet':
                description = f"Internet subscription ₦{amount:,.2f}"
                if bill_provider:
                    description = f"Internet ₦{amount:,.2f} - {bill_provider}"
                category = "Utilities"
                
            elif bill_category == 'transportation':
                description = f"Transportation payment ₦{amount:,.2f}"
                if bill_provider:
                    description = f"Transportation ₦{amount:,.2f} - {bill_provider}"
                category = "Transportation"
                
            else:
                description = f"Bill payment ₦{amount:,.2f}"
                if bill_provider:
                    description = f"Bill payment ₦{amount:,.2f} - {bill_provider}"
                category = "Utilities"
                
        elif txn_type in ['BVN_VERIFICATION', 'NIN_VERIFICATION']:
            verification_type = 'BVN' if txn_type == 'BVN_VERIFICATION' else 'NIN'
            description = f"{verification_type} verification ₦{amount:,.2f}"
            category = "Services"
            
        else:
            # Fallback for unknown types
            clean_type = txn_type.replace('_', ' ').title()
            description = f"{clean_type} ₦{amount:,.2f}"
            category = "Services"
            
        return description, category

    # ==================== TRANSACTION ENDPOINTS ====================
    
    @vas_bp.route('/transactions/all', methods=['GET'])
    @token_required
    def get_all_user_transactions(current_user):
        """Get all user transactions (VAS + Income + Expenses) in unified chronological order"""
        try:
            user_id = str(current_user['_id'])
            limit = int(request.args.get('limit', 50))
            skip = int(request.args.get('skip', 0))
            
            all_transactions = []
            
            # ────────────────────────────────────────────────────────────────
            # 1. VAS Transactions - strict type filter
            # ────────────────────────────────────────────────────────────────
            vas_query = {
                'userId': ObjectId(user_id),
                'type': {
                    '$in': [
                        'AIRTIME', 'DATA', 'BILL', 'WALLET_FUNDING',
                        'REFUND_CORRECTION', 'FEE_REFUND', 'KYC_VERIFICATION',
                        'ACTIVATION_FEE', 'SUBSCRIPTION_FEE'
                    ]
                }
            }
            vas_cursor = mongo.db.vas_transactions.find(vas_query).sort('createdAt', -1)
            vas_transactions = list(vas_cursor)
            print(f"[VAS] Found {len(vas_transactions)} records for user {user_id}")
            
            for txn in vas_transactions:
                description, category = get_transaction_display_info(txn)
                created_at = txn.get('createdAt')
                if not isinstance(created_at, datetime):
                    created_at = datetime.utcnow()
                    print(f"[VAS WARN] Invalid createdAt for txn {txn['_id']} - using now")
                
                all_transactions.append({
                    '_id': str(txn['_id']),
                    'type': 'VAS',
                    'subtype': txn.get('type', 'UNKNOWN'),
                    'amount': txn.get('amount', 0),
                    'amountPaid': txn.get('amountPaid', 0),
                    'fee': txn.get('depositFee', 0),
                    'description': description,
                    'reference': txn.get('reference', ''),
                    'status': txn.get('status', 'UNKNOWN'),
                    'provider': txn.get('provider', ''),
                    'createdAt': created_at.isoformat() + 'Z',
                    'date': created_at.isoformat() + 'Z',
                    'category': category,
                    'metadata': {
                        'phoneNumber': txn.get('phoneNumber', ''),
                        'planName': txn.get('planName', ''),
                    }
                })
            
            # ────────────────────────────────────────────────────────────────
            # 2. Income Transactions
            # ────────────────────────────────────────────────────────────────
            income_cursor = mongo.db.incomes.find({'userId': ObjectId(user_id)}).sort('createdAt', -1)
            income_transactions = list(income_cursor)
            print(f"[INCOME] Found {len(income_transactions)} records for user {user_id}")
            
            for txn in income_transactions:
                created_at = txn.get('createdAt')
                if not isinstance(created_at, datetime):
                    created_at = datetime.utcnow()
                    print(f"[INCOME WARN] Invalid createdAt for income {txn['_id']} - using now")
                
                all_transactions.append({
                    '_id': str(txn['_id']),
                    'type': 'INCOME',
                    'subtype': 'INCOME',
                    'amount': txn.get('amount', 0),
                    'description': txn.get('description', 'Income received'),
                    'title': txn.get('source', 'Income'),
                    'source': txn.get('source', 'Unknown'),
                    'reference': '',
                    'status': 'SUCCESS',
                    'createdAt': created_at.isoformat() + 'Z',
                    'date': created_at.isoformat() + 'Z',
                    'category': txn.get('category', 'Income')
                })
            
            # ────────────────────────────────────────────────────────────────
            # 3. Expense Transactions
            # ────────────────────────────────────────────────────────────────
            expense_cursor = mongo.db.expenses.find({'userId': ObjectId(user_id)}).sort('createdAt', -1)
            expense_transactions = list(expense_cursor)
            print(f"[EXPENSE] Found {len(expense_transactions)} records for user {user_id}")
            
            for txn in expense_transactions:
                created_at = txn.get('createdAt')
                if not isinstance(created_at, datetime):
                    created_at = datetime.utcnow()
                    print(f"[EXPENSE WARN] Invalid createdAt for expense {txn['_id']} - using now")
                
                all_transactions.append({
                    '_id': str(txn['_id']),
                    'type': 'EXPENSE',
                    'subtype': 'EXPENSE',
                    'amount': -txn.get('amount', 0),
                    'description': txn.get('description', 'Expense recorded'),
                    'title': txn.get('title', 'Expense'),
                    'reference': '',
                    'status': 'SUCCESS',
                    'createdAt': created_at.isoformat() + 'Z',
                    'date': created_at.isoformat() + 'Z',
                    'category': txn.get('category', 'Expense')
                })
            
            # ────────────────────────────────────────────────────────────────
            # Final sort (newest first) + pagination
            # ────────────────────────────────────────────────────────────────
            all_transactions.sort(key=lambda x: x['createdAt'], reverse=True)
            paginated = all_transactions[skip:skip + limit]
            
            print(f"[SUMMARY] Total: {len(all_transactions)} | Paginated: {len(paginated)}")
            if paginated:
                types = [t['type'] for t in paginated[:5]]
                print(f"[SUMMARY] First 5 types: {types}")
            
            return jsonify({
                'success': True,
                'data': paginated,
                'total': len(all_transactions),
                'limit': limit,
                'skip': skip,
                'message': 'All transactions loaded successfully'
            }), 200
            
        except Exception as e:
            print(f"[ERROR] /vas/transactions/all failed: {str(e)}")
            import traceback
            print(traceback.format_exc())
            return jsonify({
                'success': False,
                'message': 'Failed to load transactions',
                'error': str(e)
            }), 500

    @vas_bp.route('/transactions', methods=['GET'])
    @token_required
    def get_vas_transactions(current_user):
        """Get user's VAS transaction history"""
        try:
            user_id = str(current_user['_id'])
            
            limit = int(request.args.get('limit', 50))
            skip = int(request.args.get('skip', 0))
            transaction_type = request.args.get('type', None)
            
            query = {'userId': ObjectId(user_id)}
            if transaction_type:
                query['type'] = transaction_type.upper()
            
            transactions = list(
                mongo.db.vas_transactions.find(query)
                .sort('createdAt', -1)
                .skip(skip)
                .limit(limit)
            )
            
            serialized_transactions = []
            for txn in transactions:
                txn_data = serialize_doc(txn)
                txn_data['createdAt'] = txn.get('createdAt', datetime.utcnow()).isoformat() + 'Z'
                serialized_transactions.append(txn_data)
            
            return jsonify({
                'success': True,
                'data': serialized_transactions,
                'message': 'Transactions retrieved successfully'
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting transactions: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve transactions',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/reserved-account/create', methods=['POST'])
    @token_required
    def create_reserved_account(current_user):
        """Create a basic reserved account for the user (without KYC)"""
        try:
            user_id = str(current_user['_id'])
            
            # Check if wallet already exists
            existing_wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if existing_wallet:
                return jsonify({
                    'success': True,
                    'data': {
                        'accountNumber': existing_wallet.get('accounts', [{}])[0].get('accountNumber', ''),
                        'accountName': existing_wallet.get('accounts', [{}])[0].get('accountName', ''),
                        'bankName': existing_wallet.get('accounts', [{}])[0].get('bankName', 'Wema Bank'),
                        'bankCode': existing_wallet.get('accounts', [{}])[0].get('bankCode', '035'),
                        'createdAt': existing_wallet.get('createdAt', datetime.utcnow()).isoformat() + 'Z'
                    },
                    'message': 'Reserved account already exists'
                }), 200
            
            # Get Monnify access token
            access_token = call_monnify_auth()
            
            # Create basic reserved account (Tier 1 - no BVN/NIN required)
            account_data = {
                'accountReference': f'FICORE_{user_id}',
                'accountName': current_user.get('fullName', f"FiCore User {user_id[:8]}")[:50],  # Monnify 50-char limit
                'currencyCode': 'NGN',
                'contractCode': MONNIFY_CONTRACT_CODE,
                'customerEmail': current_user.get('email', ''),
                'customerName': current_user.get('fullName', f"FiCore User {user_id[:8]}")[:50],  # Monnify 50-char limit
                'getAllAvailableBanks': True  # Moniepoint default, user choice
            }
            
            van_response = requests.post(
                f'{MONNIFY_BASE_URL}/api/v2/bank-transfer/reserved-accounts',
                headers={
                    'Authorization': f'Bearer {access_token}',
                    'Content-Type': 'application/json'
                },
                json=account_data,
                timeout=30
            )
            
            if van_response.status_code != 200:
                raise Exception(f'Reserved account creation failed: {van_response.text}')
            
            van_data = van_response.json()['responseBody']
            
            # Create wallet record
            wallet_data = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'balance': 0.0,
                'accountReference': van_data['accountReference'],
                'contractCode': van_data['contractCode'],
                'accounts': van_data['accounts'],
                'status': 'ACTIVE',
                'tier': 'TIER_1',  # Basic account without KYC
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.vas_wallets.insert_one(wallet_data)
            
            print(f'✅ Basic reserved account created for user {user_id}')
            
            # Return all accounts for frontend to choose from
            return jsonify({
                'success': True,
                'data': {
                    'accounts': van_data['accounts'],  # All available bank accounts
                    'accountReference': van_data['accountReference'],
                    'contractCode': van_data['contractCode'],
                    'tier': 'TIER_1',
                    'kycVerified': False,
                    'createdAt': wallet_data['createdAt'].isoformat() + 'Z',
                    # Keep backward compatibility - return first account as default
                    'defaultAccount': {
                        'accountNumber': van_data['accounts'][0].get('accountNumber', '') if van_data['accounts'] else '',
                        'accountName': van_data['accounts'][0].get('accountName', '') if van_data['accounts'] else '',
                        'bankName': van_data['accounts'][0].get('bankName', 'Wema Bank') if van_data['accounts'] else 'Wema Bank',
                        'bankCode': van_data['accounts'][0].get('bankCode', '035') if van_data['accounts'] else '035',
                    }
                },
                'message': f'Reserved account created successfully with {len(van_data["accounts"])} available banks'
            }), 201
            
        except Exception as e:
            print(f'❌ Error creating reserved account: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to create reserved account',
                'errors': {'general': [str(e)]}
            }), 500
    
    def _get_reserved_accounts_with_banks_logic(current_user):
        """Business logic for getting user's reserved accounts with available banks"""
        try:
            user_id = str(current_user['_id'])
            
            # Get user's reserved account
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if not wallet:
                return {
                    'success': False,
                    'message': 'No wallet found',
                    'data': None
                }, 404
            
            # Get accounts from wallet (correct field name)
            accounts = wallet.get('accounts', [])
            if not accounts:
                return {
                    'success': False,
                    'message': 'No accounts found',
                    'data': None
                }, 404
            
            # Get preferred bank info
            preferred_bank_code = wallet.get('preferredBankCode')
            preferred_bank = None
            
            if preferred_bank_code:
                for account in accounts:
                    if account.get('bankCode') == preferred_bank_code:
                        preferred_bank = account
                        break
            
            # If no preferred bank set, use first account as default
            if not preferred_bank and accounts:
                preferred_bank = accounts[0]
            
            # Return accounts with bank information
            return {
                'success': True,
                'data': {
                    'accounts': accounts,
                    'availableBanks': accounts,  # Same as accounts for compatibility
                    'preferredBank': preferred_bank,
                    'preferredBankCode': wallet.get('preferredBankCode'),
                    'hasMultipleBanks': len(accounts) > 1
                },
                'message': 'Reserved accounts retrieved successfully'
            }, 200
            
        except Exception as e:
            print(f'❌ Error getting reserved accounts with banks: {str(e)}')
            return {
                'success': False,
                'message': 'Failed to retrieve reserved accounts',
                'errors': {'general': [str(e)]}
            }, 500

    @vas_bp.route('/reserved-accounts', methods=['GET'])
    @token_required
    def get_reserved_accounts(current_user):
        """Get user's reserved accounts (alias for backward compatibility)"""
        # Call the business logic function
        result, status_code = _get_reserved_accounts_with_banks_logic(current_user)
        return jsonify(result), status_code
    
    @vas_bp.route('/reserved-accounts/with-banks', methods=['GET'])
    @token_required
    def get_reserved_accounts_with_banks(current_user):
        """Get user's reserved accounts with available banks"""
        # Call the business logic function
        result, status_code = _get_reserved_accounts_with_banks_logic(current_user)
        return jsonify(result), status_code

    @vas_bp.route('/reserved-account', methods=['GET'])
    @token_required
    def get_reserved_account(current_user):
        """Get user's reserved account details with all available banks"""
        try:
            user_id = str(current_user['_id'])
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            
            if not wallet:
                return jsonify({
                    'success': False,
                    'message': 'Reserved account not found. Please create a wallet first.'
                }), 404
            
            # Get all available accounts
            accounts = wallet.get('accounts', [])
            
            if not accounts:
                return jsonify({
                    'success': False,
                    'message': 'No accounts found in wallet'
                }), 404
            
            # Return all accounts for frontend to choose from
            return jsonify({
                'success': True,
                'data': {
                    'accounts': accounts,  # All available bank accounts
                    'accountReference': wallet.get('accountReference', ''),
                    'status': wallet.get('status', 'active'),
                    'tier': wallet.get('tier', 'TIER_1'),
                    'kycVerified': wallet.get('kycVerified', False),
                    'createdAt': wallet.get('createdAt', datetime.utcnow()).isoformat() + 'Z',
                    # Keep backward compatibility - return first account as default
                    'defaultAccount': {
                        'accountNumber': accounts[0].get('accountNumber', ''),
                        'accountName': accounts[0].get('accountName', ''),
                        'bankName': accounts[0].get('bankName', 'Wema Bank'),
                        'bankCode': accounts[0].get('bankCode', '035'),
                    }
                },
                'message': f'Reserved account retrieved successfully with {len(accounts)} available banks'
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting reserved account: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve reserved account',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/reserved-account/set-preferred-bank', methods=['POST'])
    @token_required
    def set_preferred_bank(current_user):
        """Set user's preferred bank for their reserved account"""
        try:
            user_id = str(current_user['_id'])
            data = request.get_json()
            
            if not data or 'bankCode' not in data:
                return jsonify({
                    'success': False,
                    'message': 'Bank code is required'
                }), 400
            
            bank_code = data['bankCode']
            
            # Get user's wallet
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            if not wallet:
                return jsonify({
                    'success': False,
                    'message': 'Wallet not found'
                }), 404
            
            # Find the selected bank account
            accounts = wallet.get('accounts', [])
            selected_account = None
            
            for account in accounts:
                if account.get('bankCode') == bank_code:
                    selected_account = account
                    break
            
            if not selected_account:
                return jsonify({
                    'success': False,
                    'message': 'Bank not found in your available accounts'
                }), 404
            
            # Update user's preferred bank
            mongo.db.vas_wallets.update_one(
                {'userId': ObjectId(user_id)},
                {
                    '$set': {
                        'preferredBankCode': bank_code,
                        'updatedAt': datetime.utcnow()
                    }
                }
            )
            
            print(f'✅ User {user_id} set preferred bank to {selected_account.get("bankName")} ({bank_code})')
            
            return jsonify({
                'success': True,
                'data': {
                    'preferredAccount': selected_account,
                    'message': f'Preferred bank set to {selected_account.get("bankName")}'
                },
                'message': 'Preferred bank updated successfully'
            }), 200
            
        except Exception as e:
            print(f'❌ Error setting preferred bank: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to set preferred bank',
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
            url = f"{MONNIFY_BASE_URL}/api/v1/bank-transfer/reserved-accounts/add-linked-accounts/{reserved_account_ref}"
            
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
                
                if monnify_data.get('requestSuccessful'):
                    response_body = monnify_data.get('responseBody', {})
                    accounts = response_body.get('accounts', [])
                    
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
                    error_msg = monnify_data.get('responseMessage', 'Failed to add linked accounts')
                    print(f'🏦 Monnify error: {error_msg}')
                    return jsonify({
                        'success': False,
                        'message': error_msg
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

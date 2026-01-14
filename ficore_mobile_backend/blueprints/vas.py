"""
VAS (Value Added Services) Blueprint - Production Grade
Handles wallet management and utility purchases (Airtime, Data, etc.)

Security: API keys in environment variables, idempotency protection, webhook verification
Providers: Monnify (wallet), Peyflex (primary VAS), VTpass (backup)
"""

from flask import Blueprint, request, jsonify
from datetime import datetime, timedelta
from bson import ObjectId
import os
import requests
import hmac
import hashlib
import uuid

def init_vas_blueprint(mongo, token_required, serialize_doc):
    vas_bp = Blueprint('vas', __name__, url_prefix='/api/vas')
    
    # Environment variables (NEVER hardcode these)
    MONNIFY_API_KEY = os.environ.get('MONNIFY_API_KEY', '')
    MONNIFY_SECRET_KEY = os.environ.get('MONNIFY_SECRET_KEY', '')
    MONNIFY_CONTRACT_CODE = os.environ.get('MONNIFY_CONTRACT_CODE', '')
    MONNIFY_BASE_URL = os.environ.get('MONNIFY_BASE_URL', 'https://sandbox.monnify.com')
    
    PEYFLEX_API_TOKEN = os.environ.get('PEYFLEX_API_TOKEN', '')
    PEYFLEX_BASE_URL = os.environ.get('PEYFLEX_BASE_URL', 'https://client.peyflex.com.ng')
    
    VTPASS_API_KEY = os.environ.get('VTPASS_API_KEY', '')
    VTPASS_PUBLIC_KEY = os.environ.get('VTPASS_PUBLIC_KEY', '')
    VTPASS_SECRET_KEY = os.environ.get('VTPASS_SECRET_KEY', '')
    VTPASS_BASE_URL = os.environ.get('VTPASS_BASE_URL', 'https://sandbox.vtpass.com')
    
    VAS_TRANSACTION_FEE = 30.0
    
    # ==================== HELPER FUNCTIONS ====================
    
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
    
    def call_peyflex_airtime(network, amount, phone_number, request_id):
        """Call Peyflex Airtime API with proper headers and bypass flag"""
        payload = {
            'network': network.lower(),
            'amount': int(amount),
            'mobile_number': phone_number,
            'bypass': False,
            'request_id': request_id
        }
        
        response = requests.post(
            f'{PEYFLEX_BASE_URL}/api/airtime/topup/',
            headers={
                'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Peyflex API error: {response.status_code} - {response.text}')
    
    def call_vtpass_airtime(network, amount, phone_number, request_id):
        """Call VTpass Airtime API (fallback)"""
        network_map = {
            'MTN': 'mtn',
            'AIRTEL': 'airtel',
            'GLO': 'glo',
            '9MOBILE': 'etisalat'
        }
        
        payload = {
            'serviceID': network_map.get(network, network.lower()),
            'amount': int(amount),
            'phone': phone_number,
            'request_id': request_id
        }
        
        response = requests.post(
            f'{VTPASS_BASE_URL}/api/pay',
            headers={
                'api-key': VTPASS_API_KEY,
                'public-key': VTPASS_PUBLIC_KEY,
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '000':
                return data
            else:
                raise Exception(f'VTpass error: {data.get("response_description", "Unknown error")}')
        else:
            raise Exception(f'VTpass API error: {response.status_code} - {response.text}')
    
    def call_peyflex_data(network, data_plan_id, phone_number, request_id):
        """Call Peyflex Data API with proper headers"""
        payload = {
            'network': network.lower(),
            'plan_id': data_plan_id,
            'mobile_number': phone_number,
            'bypass': False,
            'request_id': request_id
        }
        
        response = requests.post(
            f'{PEYFLEX_BASE_URL}/api/data/purchase/',
            headers={
                'Authorization': f'Token {PEYFLEX_API_TOKEN}',
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(f'Peyflex API error: {response.status_code} - {response.text}')
    
    def call_vtpass_data(network, data_plan_code, phone_number, request_id):
        """Call VTpass Data API (fallback)"""
        network_map = {
            'MTN': 'mtn-data',
            'AIRTEL': 'airtel-data',
            'GLO': 'glo-data',
            '9MOBILE': 'etisalat-data'
        }
        
        payload = {
            'serviceID': network_map.get(network, f'{network.lower()}-data'),
            'billersCode': phone_number,
            'variation_code': data_plan_code,
            'phone': phone_number,
            'request_id': request_id
        }
        
        response = requests.post(
            f'{VTPASS_BASE_URL}/api/pay',
            headers={
                'api-key': VTPASS_API_KEY,
                'public-key': VTPASS_PUBLIC_KEY,
                'Content-Type': 'application/json'
            },
            json=payload,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            if data.get('code') == '000':
                return data
            else:
                raise Exception(f'VTpass error: {data.get("response_description", "Unknown error")}')
        else:
            raise Exception(f'VTpass API error: {response.status_code} - {response.text}')
    
    # ==================== WALLET ENDPOINTS ====================
    
    @vas_bp.route('/wallet/create', methods=['POST'])
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
                'accountName': f"{current_user.get('firstName', '')} {current_user.get('lastName', '')}".strip(),
                'currencyCode': 'NGN',
                'contractCode': MONNIFY_CONTRACT_CODE,
                'customerEmail': current_user.get('email', ''),
                'customerName': f"{current_user.get('firstName', '')} {current_user.get('lastName', '')}".strip(),
                'getAllAvailableBanks': False,
                'preferredBanks': ['035']
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
    
    @vas_bp.route('/wallet/webhook', methods=['POST'])
    def monnify_webhook():
        """Handle Monnify webhook with HMAC-SHA512 signature verification"""
        try:
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
            event_type = data.get('eventType')
            
            print(f'📥 Monnify webhook received: {event_type}')
            
            if event_type == 'SUCCESSFUL_TRANSACTION':
                transaction_data = data.get('eventData', {})
                account_reference = transaction_data.get('accountReference', '')
                amount_paid = float(transaction_data.get('amountPaid', 0))
                transaction_reference = transaction_data.get('transactionReference', '')
                
                if not account_reference.startswith('FICORE_'):
                    print(f'⚠️ Invalid account reference: {account_reference}')
                    return jsonify({'success': False, 'message': 'Invalid account reference'}), 400
                
                user_id = account_reference.replace('FICORE_', '')
                
                # Check for duplicate webhook (idempotency)
                existing_txn = mongo.db.vas_transactions.find_one({
                    'reference': transaction_reference,
                    'type': 'WALLET_FUNDING'
                })
                
                if existing_txn:
                    print(f'⚠️ Duplicate webhook ignored: {transaction_reference}')
                    return jsonify({'success': True, 'message': 'Already processed'}), 200
                
                wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
                if not wallet:
                    print(f'❌ Wallet not found for user: {user_id}')
                    return jsonify({'success': False, 'message': 'Wallet not found'}), 404
                
                new_balance = wallet.get('balance', 0.0) + amount_paid
                
                mongo.db.vas_wallets.update_one(
                    {'userId': ObjectId(user_id)},
                    {'$set': {'balance': new_balance, 'updatedAt': datetime.utcnow()}}
                )
                
                transaction = {
                    '_id': ObjectId(),
                    'userId': ObjectId(user_id),
                    'type': 'WALLET_FUNDING',
                    'amount': amount_paid,
                    'reference': transaction_reference,
                    'status': 'SUCCESS',
                    'provider': 'monnify',
                    'metadata': transaction_data,
                    'createdAt': datetime.utcnow()
                }
                
                mongo.db.vas_transactions.insert_one(transaction)
                
                print(f'✅ Wallet funded: User {user_id}, Amount: ₦{amount_paid}, New Balance: ₦{new_balance}')
                return jsonify({'success': True, 'message': 'Wallet funded successfully'}), 200
            
            return jsonify({'success': True, 'message': 'Event received'}), 200
            
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
        """Purchase airtime with idempotency protection"""
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
            
            # CRITICAL: Check for pending duplicate transaction (idempotency)
            pending_txn = check_pending_transaction(user_id, 'AIRTIME', amount, phone_number)
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
            
            is_premium = current_user.get('subscriptionStatus') == 'active'
            transaction_fee = 0.0 if is_premium else VAS_TRANSACTION_FEE
            total_amount = amount + transaction_fee
            
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
                'amount': amount,
                'transactionFee': transaction_fee,
                'totalAmount': total_amount,
                'status': 'PENDING',
                'provider': None,
                'requestId': request_id,
                'createdAt': datetime.utcnow()
            }
            
            mongo.db.vas_transactions.insert_one(vas_transaction)
            transaction_id = vas_transaction['_id']
            
            success = False
            provider = 'peyflex'
            error_message = ''
            api_response = None
            
            try:
                api_response = call_peyflex_airtime(network, amount, phone_number, request_id)
                success = True
                print(f'✅ Peyflex airtime purchase successful: {request_id}')
            except Exception as peyflex_error:
                print(f'⚠️ Peyflex failed: {str(peyflex_error)}')
                error_message = str(peyflex_error)
                
                try:
                    api_response = call_vtpass_airtime(network, amount, phone_number, request_id)
                    provider = 'vtpass'
                    success = True
                    print(f'✅ VTpass airtime purchase successful (fallback): {request_id}')
                except Exception as vtpass_error:
                    print(f'❌ VTpass failed: {str(vtpass_error)}')
                    error_message = f'Both providers failed. Peyflex: {peyflex_error}, VTpass: {vtpass_error}'
            
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
            
            # Deduct from wallet
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
            
            # Auto-create expense entry (auto-bookkeeping)
            expense_entry = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'amount': amount,
                'category': 'Utilities',
                'description': f'Airtime - {network} for {phone_number[-4:]}****',
                'date': datetime.utcnow(),
                'tags': ['VAS', 'Airtime', network],
                'vasTransactionId': transaction_id,
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.expenses.insert_one(expense_entry)
            
            print(f'✅ Airtime purchase complete: User {user_id}, Amount: ₦{amount}, Provider: {provider}')
            
            return jsonify({
                'success': True,
                'data': {
                    'transactionId': str(transaction_id),
                    'requestId': request_id,
                    'amount': amount,
                    'transactionFee': transaction_fee,
                    'totalAmount': total_amount,
                    'newBalance': new_balance,
                    'provider': provider,
                    'expenseRecorded': True
                },
                'message': 'Airtime purchased successfully! Transaction recorded as expense.'
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
        """Purchase data with idempotency protection"""
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
            
            # CRITICAL: Check for pending duplicate transaction (idempotency)
            pending_txn = check_pending_transaction(user_id, 'DATA', amount, phone_number)
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
            
            is_premium = current_user.get('subscriptionStatus') == 'active'
            transaction_fee = 0.0 if is_premium else VAS_TRANSACTION_FEE
            total_amount = amount + transaction_fee
            
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
                'amount': amount,
                'transactionFee': transaction_fee,
                'totalAmount': total_amount,
                'status': 'PENDING',
                'provider': None,
                'requestId': request_id,
                'createdAt': datetime.utcnow()
            }
            
            mongo.db.vas_transactions.insert_one(vas_transaction)
            transaction_id = vas_transaction['_id']
            
            success = False
            provider = 'peyflex'
            error_message = ''
            api_response = None
            
            try:
                api_response = call_peyflex_data(network, data_plan_id, phone_number, request_id)
                success = True
                print(f'✅ Peyflex data purchase successful: {request_id}')
            except Exception as peyflex_error:
                print(f'⚠️ Peyflex failed: {str(peyflex_error)}')
                error_message = str(peyflex_error)
                
                try:
                    api_response = call_vtpass_data(network, data_plan_id, phone_number, request_id)
                    provider = 'vtpass'
                    success = True
                    print(f'✅ VTpass data purchase successful (fallback): {request_id}')
                except Exception as vtpass_error:
                    print(f'❌ VTpass failed: {str(vtpass_error)}')
                    error_message = f'Both providers failed. Peyflex: {peyflex_error}, VTpass: {vtpass_error}'
            
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
            
            # Deduct from wallet
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
            
            # Auto-create expense entry (auto-bookkeeping)
            expense_entry = {
                '_id': ObjectId(),
                'userId': ObjectId(user_id),
                'amount': amount,
                'category': 'Utilities',
                'description': f'Data - {network} {data_plan_name} for {phone_number[-4:]}****',
                'date': datetime.utcnow(),
                'tags': ['VAS', 'Data', network],
                'vasTransactionId': transaction_id,
                'createdAt': datetime.utcnow(),
                'updatedAt': datetime.utcnow()
            }
            
            mongo.db.expenses.insert_one(expense_entry)
            
            print(f'✅ Data purchase complete: User {user_id}, Amount: ₦{amount}, Provider: {provider}')
            
            return jsonify({
                'success': True,
                'data': {
                    'transactionId': str(transaction_id),
                    'requestId': request_id,
                    'amount': amount,
                    'transactionFee': transaction_fee,
                    'totalAmount': total_amount,
                    'newBalance': new_balance,
                    'provider': provider,
                    'expenseRecorded': True
                },
                'message': 'Data purchased successfully! Transaction recorded as expense.'
            }), 200
            
        except Exception as e:
            print(f'❌ Error buying data: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to purchase data',
                'errors': {'general': [str(e)]}
            }), 500
    
    @vas_bp.route('/networks/airtime', methods=['GET'])
    def get_airtime_networks():
        """Get list of supported airtime networks"""
        try:
            response = requests.get(
                f'{PEYFLEX_BASE_URL}/api/airtime/networks/',
                timeout=10
            )
            
            if response.status_code == 200:
                return jsonify({
                    'success': True,
                    'data': response.json(),
                    'message': 'Networks retrieved successfully'
                }), 200
            else:
                return jsonify({
                    'success': True,
                    'data': [
                        {'id': 'mtn', 'name': 'MTN'},
                        {'id': 'airtel', 'name': 'Airtel'},
                        {'id': 'glo', 'name': 'Glo'},
                        {'id': '9mobile', 'name': '9mobile'}
                    ],
                    'message': 'Default networks list'
                }), 200
        except Exception as e:
            print(f'⚠️ Error getting networks: {str(e)}')
            return jsonify({
                'success': True,
                'data': [
                    {'id': 'mtn', 'name': 'MTN'},
                    {'id': 'airtel', 'name': 'Airtel'},
                    {'id': 'glo', 'name': 'Glo'},
                    {'id': '9mobile', 'name': '9mobile'}
                ],
                'message': 'Default networks list'
            }), 200
    
    @vas_bp.route('/networks/data', methods=['GET'])
    def get_data_networks():
        """Get list of supported data networks"""
        try:
            response = requests.get(
                f'{PEYFLEX_BASE_URL}/api/data/networks/',
                timeout=10
            )
            
            if response.status_code == 200:
                return jsonify({
                    'success': True,
                    'data': response.json(),
                    'message': 'Networks retrieved successfully'
                }), 200
            else:
                return jsonify({
                    'success': True,
                    'data': [
                        {'id': 'mtn', 'name': 'MTN'},
                        {'id': 'airtel', 'name': 'Airtel'},
                        {'id': 'glo', 'name': 'Glo'},
                        {'id': '9mobile', 'name': '9mobile'}
                    ],
                    'message': 'Default networks list'
                }), 200
        except Exception as e:
            print(f'⚠️ Error getting networks: {str(e)}')
            return jsonify({
                'success': True,
                'data': [
                    {'id': 'mtn', 'name': 'MTN'},
                    {'id': 'airtel', 'name': 'Airtel'},
                    {'id': 'glo', 'name': 'Glo'},
                    {'id': '9mobile', 'name': '9mobile'}
                ],
                'message': 'Default networks list'
            }), 200
    
    @vas_bp.route('/data-plans/<network>', methods=['GET'])
    def get_data_plans(network):
        """Get data plans for a specific network"""
        try:
            response = requests.get(
                f'{PEYFLEX_BASE_URL}/api/data/plans/?network={network.lower()}',
                timeout=10
            )
            
            if response.status_code == 200:
                return jsonify({
                    'success': True,
                    'data': response.json(),
                    'message': 'Data plans retrieved successfully'
                }), 200
            else:
                return jsonify({
                    'success': False,
                    'message': 'Failed to retrieve data plans',
                    'errors': {'general': ['Provider API error']}
                }), 500
        except Exception as e:
            print(f'❌ Error getting data plans: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve data plans',
                'errors': {'general': [str(e)]}
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
    
    @vas_bp.route('/reserved-account', methods=['GET'])
    @token_required
    def get_reserved_account(current_user):
        """Get user's reserved account details"""
        try:
            user_id = str(current_user['_id'])
            wallet = mongo.db.vas_wallets.find_one({'userId': ObjectId(user_id)})
            
            if not wallet:
                return jsonify({
                    'success': False,
                    'message': 'Reserved account not found. Please create a wallet first.'
                }), 404
            
            return jsonify({
                'success': True,
                'data': {
                    'accountReference': wallet.get('accountReference', ''),
                    'accountName': wallet.get('accountName', ''),
                    'accounts': wallet.get('accounts', []),
                    'status': wallet.get('status', 'active'),
                    'createdAt': wallet.get('createdAt', datetime.utcnow()).isoformat() + 'Z'
                },
                'message': 'Reserved account retrieved successfully'
            }), 200
            
        except Exception as e:
            print(f'❌ Error getting reserved account: {str(e)}')
            return jsonify({
                'success': False,
                'message': 'Failed to retrieve reserved account',
                'errors': {'general': [str(e)]}
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
    
    return vas_bp

# -----------------------------------------------------------------------
# transaction_db.py
# Author: Implementation for transaction durability
# -----------------------------------------------------------------------
# This module implements transaction durability through logging
# with before-images, after-images, active and committed transaction tables
# -----------------------------------------------------------------------

import struct
import ctypes
import os
import time
from common_db import BLOCK_SIZE

# Transaction states
TRANS_ACTIVE = 1
TRANS_COMMITTED = 2
TRANS_ABORTED = 3

# Log record types
LOG_BEGIN = 1
LOG_BEFORE_IMAGE = 2
LOG_AFTER_IMAGE = 3
LOG_COMMIT = 4
LOG_ABORT = 5

class TransactionManager:
    def __init__(self):
        self.current_transaction_id = 0
        self.active_transactions = {}  # trans_id -> transaction_info
        self.committed_transactions = set()
        
        # Initialize log files
        self.before_image_file = "before_images.log"
        self.after_image_file = "after_images.log"
        self.transaction_log_file = "transaction.log"
        
        self._init_log_files()
        
    def _init_log_files(self):
        """Initialize log files if they don't exist"""
        for filename in [self.before_image_file, self.after_image_file, self.transaction_log_file]:
            if not os.path.exists(filename):
                with open(filename, 'wb') as f:
                    # Write empty header block
                    header = ctypes.create_string_buffer(BLOCK_SIZE)
                    struct.pack_into('!ii', header, 0, 0, 0)  # next_block_id, num_records
                    f.write(header)
    
    def begin_transaction(self):
        """Begin a new transaction"""
        self.current_transaction_id += 1
        trans_id = self.current_transaction_id
        
        self.active_transactions[trans_id] = {
            'start_time': time.time(),
            'state': TRANS_ACTIVE,
            'operations': []
        }
        
        # Log transaction begin
        self._write_transaction_log(trans_id, LOG_BEGIN, None)
        print(f"Transaction {trans_id} started")
        return trans_id
    
    def commit_transaction(self, trans_id):
        """Commit a transaction"""
        if trans_id not in self.active_transactions:
            print(f"Error: Transaction {trans_id} not found")
            return False
            
        # Write all after-images to ensure durability (Commit Rule)
        self._flush_after_images(trans_id)
        
        # Log transaction commit
        self._write_transaction_log(trans_id, LOG_COMMIT, None)
        
        # Move to committed transactions
        self.committed_transactions.add(trans_id)
        del self.active_transactions[trans_id]
        
        print(f"Transaction {trans_id} committed")
        return True
    
    def abort_transaction(self, trans_id):
        """Abort a transaction"""
        if trans_id not in self.active_transactions:
            print(f"Error: Transaction {trans_id} not found")
            return False
        
        # Log transaction abort
        self._write_transaction_log(trans_id, LOG_ABORT, None)
        
        # Remove from active transactions
        self.active_transactions[trans_id]['state'] = TRANS_ABORTED
        del self.active_transactions[trans_id]
        
        print(f"Transaction {trans_id} aborted")
        return True
    
    def log_before_image(self, trans_id, table_name, block_id, old_data):
        """Log before-image (Write-Ahead Logging Rule)"""
        if trans_id not in self.active_transactions:
            print(f"Error: Transaction {trans_id} not active")
            return False
        
        log_entry = {
            'trans_id': trans_id,
            'table_name': table_name,
            'block_id': block_id,
            'data': old_data,
            'timestamp': time.time()
        }
        
        self._write_to_log_file(self.before_image_file, log_entry)
        self.active_transactions[trans_id]['operations'].append(('before', log_entry))
        print(f"Before-image logged for transaction {trans_id}")
        return True
    
    def log_after_image(self, trans_id, table_name, block_id, new_data):
        """Log after-image"""
        if trans_id not in self.active_transactions:
            print(f"Error: Transaction {trans_id} not active")
            return False
        
        log_entry = {
            'trans_id': trans_id,
            'table_name': table_name,
            'block_id': block_id,
            'data': new_data,
            'timestamp': time.time()
        }
        
        self._write_to_log_file(self.after_image_file, log_entry)
        self.active_transactions[trans_id]['operations'].append(('after', log_entry))
        print(f"After-image logged for transaction {trans_id}")
        return True
    
    def _write_to_log_file(self, filename, log_entry):
        """Write log entry to specified log file"""
        with open(filename, 'r+b') as f:
            # Read header to get next available position
            f.seek(0)
            header_data = f.read(8)
            next_block_id, num_records = struct.unpack('!ii', header_data)
            
            # Serialize log entry
            table_name_bytes = log_entry['table_name'].encode('utf-8')[:50]  # Limit table name
            table_name_bytes = table_name_bytes.ljust(50, b'\0')
            
            data_bytes = log_entry['data'][:BLOCK_SIZE-100]  # Leave space for metadata
            data_len = len(data_bytes)
            
            entry_size = 50 + 4 + 4 + 8 + 4 + data_len  # table_name + trans_id + block_id + timestamp + data_len + data
            
            # Find position to write
            write_pos = BLOCK_SIZE + num_records * BLOCK_SIZE
            f.seek(write_pos)
            
            # Write log entry
            entry_buffer = ctypes.create_string_buffer(BLOCK_SIZE)
            struct.pack_into('!50siid i', entry_buffer, 0, 
                           table_name_bytes, 
                           log_entry['trans_id'],
                           log_entry['block_id'], 
                           log_entry['timestamp'],
                           data_len)
            
            struct.pack_into(f'!{data_len}s', entry_buffer, 70, data_bytes)
            f.write(entry_buffer)
            
            # Update header
            f.seek(0)
            struct.pack_into('!ii', header_data, 0, next_block_id, num_records + 1)
            f.write(header_data)
            f.flush()
    
    def _write_transaction_log(self, trans_id, log_type, data):
        """Write transaction state change to transaction log"""
        with open(self.transaction_log_file, 'a+b') as f:
            log_entry = struct.pack('!iid', trans_id, log_type, time.time())
            f.write(log_entry)
            f.flush()
    
    def _flush_after_images(self, trans_id):
        """Ensure all after-images for transaction are written to disk"""
        # In a real implementation, this would ensure all after-images
        # are written to non-volatile storage before commit
        print(f"Flushing after-images for transaction {trans_id}")
    
    def is_transaction_active(self, trans_id):
        """Check if transaction is active"""
        return trans_id in self.active_transactions
    
    def is_transaction_committed(self, trans_id):
        """Check if transaction is committed"""
        return trans_id in self.committed_transactions
    
    def get_active_transactions(self):
        """Get list of active transaction IDs"""
        return list(self.active_transactions.keys())
    
    def get_committed_transactions(self):
        """Get list of committed transaction IDs"""
        return list(self.committed_transactions)

# Global transaction manager instance
global_transaction_manager = TransactionManager()

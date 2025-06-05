'''
index_db.py
in this module, B tree is implemented
'''

import struct
import os
import common_db # Haomin Wang: Assuming common_db.BLOCK_SIZE is defined
import ctypes

# The 0 block stores the meta information of the tree
'''
block_id|has_root|num_of_levels|root_node_ptr|next_available_block_id
# note: the root_node_ptr is a block id
# Haomin Wang: Added next_available_block_id to manage block allocation
'''
# Haomin Wang: Define constants for meta block offsets
META_BLOCK_ID_OFFSET = 0
META_HAS_ROOT_OFFSET = struct.calcsize('!i')
META_NUM_LEVELS_OFFSET = META_HAS_ROOT_OFFSET + struct.calcsize('!?') # ? for bool
META_ROOT_NODE_PTR_OFFSET = META_NUM_LEVELS_OFFSET + struct.calcsize('!i')
META_NEXT_AVAIL_BLOCK_OFFSET = META_ROOT_NODE_PTR_OFFSET + struct.calcsize('!i')
META_BLOCK_HEADER_FORMAT = '!i?iii' # block_id, has_root, num_levels, root_ptr, next_avail_block

MAX_NUM_OF_KEYS=50 # Haomin Wang: Reduced for easier testing of splits. Original was 200.
                    # This should be calculated based on BLOCK_SIZE and key/pointer sizes.

# structure of node header: block_id|node_type|number_of_keys
NODE_HEADER_FORMAT = '!iii'
NODE_HEADER_SIZE = struct.calcsize(NODE_HEADER_FORMAT)

# structure of leaf node
'''
block_id|node_type|number_of_keys|key_0|ptr_0_block_id|ptr_0_offset|...|key_n|ptr_n_offset|...free space...|next_leaf_block_id
note: for leaf node, ptr is a block id+entry id (8 bytes) except for the last one (next_leaf_block_id)
key is assumed to be fixed size for simplicity here (e.g., 10 bytes string)
'''
LEAF_NODE_TYPE=1
# Haomin Wang: Define key format and size. Let's assume keys are 10-byte strings.
KEY_FORMAT = '!10s'
KEY_SIZE = struct.calcsize(KEY_FORMAT)
LEAF_POINTER_FORMAT = '!ii' # block_id, offset_in_block
LEAF_POINTER_SIZE = struct.calcsize(LEAF_POINTER_FORMAT)
# Haomin Wang: Size of one key-pointer pair in a leaf node
LEAF_ENTRY_SIZE = KEY_SIZE + LEAF_POINTER_SIZE
# Haomin Wang: Pointer to the next leaf node (for range scans)
NEXT_LEAF_BLOCK_ID_FORMAT = '!i'
NEXT_LEAF_BLOCK_ID_SIZE = struct.calcsize(NEXT_LEAF_BLOCK_ID_FORMAT)


# structure of internal node
'''
block_id|node_type|number_of_keys|ptr_0|key_1|ptr_1|...|key_n|ptr_n|...free space...
note: For internal node, ptr is just a block id (4 bytes) to another B-tree node.
An internal node with k keys has k+1 pointers.
ptr_0 | key_1 | ptr_1 | key_2 | ptr_2 | ... | key_k | ptr_k
'''
INTERNAL_NODE_TYPE=0
INTERNAL_POINTER_FORMAT = '!i' # block_id of child node
INTERNAL_POINTER_SIZE = struct.calcsize(INTERNAL_POINTER_FORMAT)
# Haomin Wang: Size of one key-pointer pair in an internal node (key_i, ptr_i)
INTERNAL_ENTRY_SIZE = KEY_SIZE + INTERNAL_POINTER_SIZE


SPECIAL_INDEX_BLOCK_PTR=-1 # this is the last ptr for last leaf node when the next node is unknown (or -1 for no next leaf)

# Haomin Wang: Function to calculate max keys for node types based on BLOCK_SIZE
def calculate_max_keys():
    # Max keys for Leaf Node:
    # BLOCK_SIZE = NODE_HEADER_SIZE + (N * LEAF_ENTRY_SIZE) + NEXT_LEAF_BLOCK_ID_SIZE
    # N = (BLOCK_SIZE - NODE_HEADER_SIZE - NEXT_LEAF_BLOCK_ID_SIZE) / LEAF_ENTRY_SIZE
    global MAX_LEAF_KEYS, MAX_INTERNAL_KEYS
    MAX_LEAF_KEYS = (common_db.BLOCK_SIZE - NODE_HEADER_SIZE - NEXT_LEAF_BLOCK_ID_SIZE) // LEAF_ENTRY_SIZE

    # Max keys for Internal Node:
    # BLOCK_SIZE = NODE_HEADER_SIZE + INTERNAL_POINTER_SIZE (for ptr_0) + N * (KEY_SIZE + INTERNAL_POINTER_SIZE)
    # N = (BLOCK_SIZE - NODE_HEADER_SIZE - INTERNAL_POINTER_SIZE) / (KEY_SIZE + INTERNAL_POINTER_SIZE)
    MAX_INTERNAL_KEYS = (common_db.BLOCK_SIZE - NODE_HEADER_SIZE - INTERNAL_POINTER_SIZE) // INTERNAL_ENTRY_SIZE

    # Ensure MAX_NUM_OF_KEYS is consistent, perhaps use MAX_LEAF_KEYS or MAX_INTERNAL_KEYS specifically
    # For simplicity, we can set MAX_NUM_OF_KEYS to the smaller of the two if used generically,
    # but it's better to use specific max for leaf and internal.
    print(f"Max Leaf Keys: {MAX_LEAF_KEYS}, Max Internal Keys: {MAX_INTERNAL_KEYS}")

# Haomin Wang: Call it once, assuming common_db.BLOCK_SIZE is available
# This needs common_db to be fully initialized. If BLOCK_SIZE can change, this might need to be dynamic.
# For now, let's assume it's fixed. This might be better inside the Index class or called after common_db setup.
# calculate_max_keys() # Call this after common_db.BLOCK_SIZE is set.

class Index(object):
    #------------------------------------
    # constructor of the class
    # input
    #       tablename : the table to be indexed
    #       indexed_field_name: the name of the field being indexed (for creating new index)
    #-----------------------------------------
    def __init__(self, tablename, indexed_field_name=None): # Haomin Wang: Added indexed_field_name
        # Haomin Wang: Calculate max keys here, ensuring common_db.BLOCK_SIZE is set
        if common_db.BLOCK_SIZE:
            calculate_max_keys()
        else:
            # Fallback or error if BLOCK_SIZE not set. For now, use a default.
            global MAX_LEAF_KEYS, MAX_INTERNAL_KEYS
            MAX_LEAF_KEYS = MAX_NUM_OF_KEYS
            MAX_INTERNAL_KEYS = MAX_NUM_OF_KEYS
            print("Warning: common_db.BLOCK_SIZE not found, using default MAX_NUM_OF_KEYS.")


        print ("__init__ of ",Index.__name__)
        self.tablename = tablename.strip()
        self.index_file_name = self.tablename + ".ind" # Haomin Wang: Store filename
        self.open = False # Haomin Wang: Initialize open flag

        # Haomin Wang: Meta information, loaded from block 0 or initialized
        self.meta_block_id = 0
        self.has_root = False
        self.num_of_levels = 0
        self.root_node_ptr = SPECIAL_INDEX_BLOCK_PTR # Block ID of the root node
        self.next_available_block_id = 1 # Block 0 is meta, so data/index nodes start from 1

        if not os.path.exists(self.index_file_name):
            print (f'Index file {self.index_file_name} does not exist. Creating...')
            self.f_handle = open(self.index_file_name, 'wb+') # Create if not exists
            self._initialize_meta_block()
            self.open = True
            print (f'{self.index_file_name} has been created and meta block initialized.')
        else:
            self.f_handle = open(self.index_file_name, 'rb+')
            self.open = True
            self._load_meta_block()
            print (f'Index file {self.index_file_name} has been opened and meta block loaded.')

    # Haomin Wang: Helper to initialize meta block (Block 0)
    def _initialize_meta_block(self):
        self.has_root = False
        self.num_of_levels = 0
        self.root_node_ptr = SPECIAL_INDEX_BLOCK_PTR # No root yet
        self.next_available_block_id = 1 # First node will be block 1

        meta_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        struct.pack_into(META_BLOCK_HEADER_FORMAT, meta_buf, 0,
                         self.meta_block_id, self.has_root, self.num_of_levels,
                         self.root_node_ptr, self.next_available_block_id)
        self.f_handle.seek(0)
        self.f_handle.write(meta_buf)
        self.f_handle.flush()

    # Haomin Wang: Helper to load meta block
    def _load_meta_block(self):
        self.f_handle.seek(0)
        meta_buf = self.f_handle.read(common_db.BLOCK_SIZE)
        if len(meta_buf) < struct.calcsize(META_BLOCK_HEADER_FORMAT):
            print("Warning: Meta block is too small. Re-initializing.")
            self._initialize_meta_block() # Re-initialize if corrupt or too small
            return

        _, self.has_root, self.num_of_levels, self.root_node_ptr, self.next_available_block_id = \
            struct.unpack_from(META_BLOCK_HEADER_FORMAT, meta_buf, 0)
        print(f"Meta loaded: HasRoot={self.has_root}, Levels={self.num_of_levels}, RootPtr={self.root_node_ptr}, NextBlockID={self.next_available_block_id}")

    # Haomin Wang: Helper to save meta block
    def _save_meta_block(self):
        if not self.open:
            print("Error: Index file not open. Cannot save meta block.")
            return
        meta_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        struct.pack_into(META_BLOCK_HEADER_FORMAT, meta_buf, 0,
                         self.meta_block_id, self.has_root, self.num_of_levels,
                         self.root_node_ptr, self.next_available_block_id)
        self.f_handle.seek(0)
        self.f_handle.write(meta_buf)
        self.f_handle.flush()

    # Haomin Wang: Allocate a new block for a B-tree node
    def _allocate_new_block_id(self):
        block_id = self.next_available_block_id
        self.next_available_block_id += 1
        self._save_meta_block() # Save change to next_available_block_id
        return block_id

    # Haomin Wang: Read a B-tree node from file
    def _read_node_block(self, block_id):
        if block_id == SPECIAL_INDEX_BLOCK_PTR or block_id < 1: # Block 0 is meta
            return None
        self.f_handle.seek(block_id * common_db.BLOCK_SIZE)
        node_buf = self.f_handle.read(common_db.BLOCK_SIZE)
        if len(node_buf) < NODE_HEADER_SIZE:
            print(f"Error: Block {block_id} is too small to be a node.")
            return None
        return node_buf

    # Haomin Wang: Write a B-tree node to file
    def _write_node_block(self, block_id, node_buf):
        if block_id == SPECIAL_INDEX_BLOCK_PTR or block_id < 1:
            print(f"Error: Cannot write to invalid block_id {block_id}")
            return
        self.f_handle.seek(block_id * common_db.BLOCK_SIZE)
        self.f_handle.write(node_buf)
        self.f_handle.flush()

    # Haomin Wang: Helper to create a new B-tree node (in memory buffer)
    def _create_new_node_buffer(self, node_block_id, node_type, num_keys=0):
        node_buf = ctypes.create_string_buffer(common_db.BLOCK_SIZE)
        # Pack header: block_id (informational, already known), node_type, num_keys
        struct.pack_into(NODE_HEADER_FORMAT, node_buf, 0, node_block_id, node_type, num_keys)
        if node_type == LEAF_NODE_TYPE:
            # Initialize next leaf pointer
            struct.pack_into(NEXT_LEAF_BLOCK_ID_FORMAT, node_buf,
                             common_db.BLOCK_SIZE - NEXT_LEAF_BLOCK_ID_SIZE,
                             SPECIAL_INDEX_BLOCK_PTR)
        return node_buf

    # Haomin Wang: Method to format the key (e.g., string to fixed-size bytes)
    def _format_key(self, key_value):
        if isinstance(key_value, str):
            key_value_bytes = key_value.encode('utf-8')
        elif isinstance(key_value, bytes):
            key_value_bytes = key_value
        else: # Attempt to convert to string then bytes for other types (e.g. int)
            key_value_bytes = str(key_value).encode('utf-8')

        # Pad or truncate to KEY_SIZE (10 bytes)
        if len(key_value_bytes) > KEY_SIZE:
            return key_value_bytes[:KEY_SIZE]
        else:
            return key_value_bytes.ljust(KEY_SIZE, b'\0') # Pad with null bytes

    #---------------------------------
    # destructor of the class
    #-----------------------------------
    def __del__(self):
        print ("__del__ of ",Index.__name__)
        if self.open:
            self._save_meta_block() # Haomin Wang: Ensure meta is saved on close
            self.f_handle.close()
            self.open = False

    #-----------------------------
    # create index for all indexed items in one run
    # This would typically read all data from the table and insert entries one by one.
    # For now, this is a placeholder.
    #-----------------------------------
    def create_index(self, storage_obj, field_to_index_name):
        print (f'create_index for field {field_to_index_name} begins to execute')
        # 1. Get field information from storage_obj to find the index of field_to_index_name
        field_list = storage_obj.getFieldList() # list of (name_bytes, type, length)
        field_idx = -1
        for i, f_info in enumerate(field_list):
            if f_info[0].decode('utf-8').strip() == field_to_index_name:
                field_idx = i
                break

        if field_idx == -1:
            print(f"Error: Field '{field_to_index_name}' not found in table schema.")
            return

        # 2. Iterate over all records in the table (via storage_obj)
        #    For each record, get the value of the field to be indexed.
        #    And get its location (block_id in .dat file, offset/entry_id in that block)
        #    This part needs storage_obj to provide record location info if possible,
        #    or we assume (block_id, offset_in_block) for data records are known.
        #    For simplicity, let's assume storage_obj.getRecordWithLocation() gives [(record_tuple, (dat_block_id, offset_in_dat_block))]

        # This is a simplified loop assuming records have a fixed structure.
        # The (data_block_id, record_offset_in_data_block) needs to be systematically obtained.
        # For now, using a placeholder for data_block_id and record_offset.
        all_records = storage_obj.getRecord() # list of tuples
        # We need a way to associate each record with its actual storage location (block, offset)
        # Let's assume for this example, the 'offset' is just the record's index in the list for simplicity,
        # and 'block_id' is a placeholder. This needs to be replaced with actual .dat file locations.

        for i, record_tuple in enumerate(all_records):
            key_value = record_tuple[field_idx] # Get the value of the indexed field
            # Placeholder for data location: (dat_file_block_id, offset_of_record_in_that_block)
            # This needs to be correctly retrieved from how storage_db.py stores records.
            # If storage_db.py's self.record_Position = [(block_num_in_dat, record_index_in_block_offsets)]
            # and each block in .dat has its own offsets table.
            # This is a complex part depending on storage_db.py structure.
            # For now, let's use (placeholder_block_id=1, placeholder_offset=i)
            data_block_id_in_dat_file = 1 # Placeholder
            offset_of_record_in_dat_block = i # Placeholder

            self.insert_index_entry(key_value, data_block_id_in_dat_file, offset_of_record_in_dat_block)

        print(f"Finished creating index for {field_to_index_name}")


    #-----------------------------
    # get the internal node to follow when searching/inserting in an internal node
    # input
    #       key_to_find: the key we are looking for
    #       internal_node_buffer: the buffer of the current internal node
    # output
    #       the block_id of the child node to follow
    #--------------------------------
    def _find_child_ptr_in_internal_node(self, key_to_find_formatted, internal_node_buffer):
        # Unpack header: _, node_type, num_keys
        _, _, num_keys = struct.unpack_from(NODE_HEADER_FORMAT, internal_node_buffer, 0)

        current_offset = NODE_HEADER_SIZE
        # First pointer (ptr_0)
        child_ptr = struct.unpack_from(INTERNAL_POINTER_FORMAT, internal_node_buffer, current_offset)[0]
        current_offset += INTERNAL_POINTER_SIZE

        for i in range(num_keys):
            # Key_i+1
            key_i_bytes = struct.unpack_from(KEY_FORMAT, internal_node_buffer, current_offset)[0]
            current_offset += KEY_SIZE

            # Pointer_i+1
            ptr_i = struct.unpack_from(INTERNAL_POINTER_FORMAT, internal_node_buffer, current_offset)[0]
            current_offset += INTERNAL_POINTER_SIZE

            if key_to_find_formatted < key_i_bytes:
                # We already have the child_ptr from the previous iteration (or ptr_0)
                return child_ptr
            else: # key_to_find_formatted >= key_i_bytes
                child_ptr = ptr_i # This child_ptr corresponds to keys >= key_i_bytes

        return child_ptr # If key_to_find is >= all keys, follow the last pointer

    #---------------------------------
    # insert the (key, (block_id, offset_id)) into a sorted list of keys and list of ptr_tuples in a leaf
    # This function modifies key_list and ptr_list in place.
    #---------------------------------
    def _insert_into_sorted_leaf_lists(self, formatted_key, ptr_tuple, key_list, ptr_list):
        pos = 0
        while pos < len(key_list) and key_list[pos] < formatted_key:
            pos += 1

        # Check for duplicate key. B-trees can handle duplicates, often by storing multiple data pointers
        # for the same key, or by making keys unique (e.g., appending a unique record ID).
        # For this basic version, if keys are meant to be unique in the index, we might raise an error
        # or overwrite. If duplicates are allowed, they would be inserted.
        # This implementation will insert, allowing duplicates side-by-side if formatted_key matches an existing key.
        # If keys should be strictly unique, an additional check is needed here.

        key_list.insert(pos, formatted_key)
        ptr_list.insert(pos, ptr_tuple)

    #-------------------------------
    # to insert a index entry into the index file
    # input
    #       key_value     # field value to be indexed
    #       data_block_id # block_id in the .dat file where the actual record is
    #       data_offset   # offset/entry_id in that .dat block for the record
    #--------------------------------------
    def insert_index_entry(self, key_value, data_block_id, data_offset):
        print(f"insert_index_entry: key='{key_value}', data_loc=({data_block_id},{data_offset})")

        formatted_key = self._format_key(key_value) # Ensure key is in standard byte format

        # Case 1: Tree is empty
        if not self.has_root:
            # Create the first leaf node, which is also the root
            root_block_id = self._allocate_new_block_id()
            self.root_node_ptr = root_block_id
            self.has_root = True
            self.num_of_levels = 1

            root_node_buf = self._create_new_node_buffer(root_block_id, LEAF_NODE_TYPE, num_keys=1)

            # Store the single key and its pointer
            # key_0
            struct.pack_into(KEY_FORMAT, root_node_buf, NODE_HEADER_SIZE, formatted_key)
            # ptr_0 (data_block_id, data_offset)
            struct.pack_into(LEAF_POINTER_FORMAT, root_node_buf,
                             NODE_HEADER_SIZE + KEY_SIZE,
                             data_block_id, data_offset)

            self._write_node_block(root_block_id, root_node_buf)
            self._save_meta_block() # num_levels, root_node_ptr, has_root updated
            print(f"Tree was empty. Created new root (leaf) at block {root_block_id}.")
            return

        # Case 2: Tree exists. Find the correct leaf node.
        # This part needs a proper search recursive function.
        # For now, this is a simplified placeholder logic from the original code.
        # It needs to be replaced by a full B-tree insertion algorithm including splits.

        # Simplified: Traverse to the leaf node (Placeholder - needs full B-tree search logic)
        # The original code had a loop `while(temp_count<self.num_of_levels-1):`
        # This needs to be a proper recursive or iterative descent.

        # For now, assume we directly access the leaf if num_levels == 1 (root is leaf)
        # Or we need a find_leaf_for_insertion type of function.
        # Let's assume a helper `_find_leaf_node(key)` returns (leaf_node_buf, leaf_block_id, path_to_leaf)
        # This is a major part of B-tree logic.

        # Placeholder: For simplicity, let's assume the root is currently always the leaf if levels = 1
        # This is not a complete B-Tree insertion if tree depth > 1 without proper traversal and splitting.

        current_node_block_id = self.root_node_ptr
        # Haomin Wang: Path tracking for splits propagating upwards (parent pointers)
        # path_to_leaf = [] # List of (block_id, buffer, index_in_parent)

        for _ in range(self.num_of_levels -1 ): # Traverse internal nodes
            internal_node_buf = self._read_node_block(current_node_block_id)
            if not internal_node_buf:
                print(f"Error: Could not read internal node at block {current_node_block_id}")
                return
            # path_to_leaf.append(current_node_block_id) # Simplified path

            # _, node_type, _ = struct.unpack_from(NODE_HEADER_FORMAT, internal_node_buf, 0)
            # if node_type != INTERNAL_NODE_TYPE:
            #    print(f"Error: Expected internal node, got type {node_type} at block {current_node_block_id}")
            #    return
            current_node_block_id = self._find_child_ptr_in_internal_node(formatted_key, internal_node_buf)

        # Now current_node_block_id should be the leaf node's block_id
        leaf_node_buf = self._read_node_block(current_node_block_id)
        if not leaf_node_buf:
            print(f"Error: Could not read leaf node at block {current_node_block_id}")
            return

        # _, node_type, num_keys = struct.unpack_from(NODE_HEADER_FORMAT, leaf_node_buf, 0)
        # if node_type != LEAF_NODE_TYPE:
        #    print(f"Error: Expected leaf node, got type {node_type} at block {current_node_block_id}")
        #    return

        # Read existing keys and pointers from the leaf node into lists
        keys_in_leaf = []
        ptrs_in_leaf = [] # list of (data_block_id, data_offset)

        # Unpack header: block_id_from_node (ignore), node_type, num_keys
        _, _, num_keys_in_node = struct.unpack_from(NODE_HEADER_FORMAT, leaf_node_buf, 0)

        current_offset = NODE_HEADER_SIZE
        for _ in range(num_keys_in_node):
            key_bytes = struct.unpack_from(KEY_FORMAT, leaf_node_buf, current_offset)[0]
            keys_in_leaf.append(key_bytes)
            current_offset += KEY_SIZE

            db_id, d_off = struct.unpack_from(LEAF_POINTER_FORMAT, leaf_node_buf, current_offset)
            ptrs_in_leaf.append((db_id, d_off))
            current_offset += LEAF_POINTER_SIZE

        # Insert new key-pointer into these lists (sorted)
        self._insert_into_sorted_leaf_lists(formatted_key, (data_block_id, data_offset), keys_in_leaf, ptrs_in_leaf)

        num_keys_after_insert = len(keys_in_leaf)

        # Check if leaf node needs splitting
        if num_keys_after_insert <= MAX_LEAF_KEYS:
            # No split needed, just update the leaf node
            # Re-create buffer or modify in place carefully. Let's re-create for clarity.
            updated_leaf_buf = self._create_new_node_buffer(current_node_block_id, LEAF_NODE_TYPE, num_keys_after_insert)

            write_offset = NODE_HEADER_SIZE
            for i in range(num_keys_after_insert):
                struct.pack_into(KEY_FORMAT, updated_leaf_buf, write_offset, keys_in_leaf[i])
                write_offset += KEY_SIZE
                struct.pack_into(LEAF_POINTER_FORMAT, updated_leaf_buf, write_offset,
                                 ptrs_in_leaf[i][0], ptrs_in_leaf[i][1])
                write_offset += LEAF_POINTER_SIZE

            # Preserve next leaf pointer (read from original buffer)
            original_next_leaf_ptr = struct.unpack_from(NEXT_LEAF_BLOCK_ID_FORMAT, leaf_node_buf,
                                                        common_db.BLOCK_SIZE - NEXT_LEAF_BLOCK_ID_SIZE)[0]
            struct.pack_into(NEXT_LEAF_BLOCK_ID_FORMAT, updated_leaf_buf,
                             common_db.BLOCK_SIZE - NEXT_LEAF_BLOCK_ID_SIZE,
                             original_next_leaf_ptr)

            self._write_node_block(current_node_block_id, updated_leaf_buf)
            print(f"Inserted key into leaf block {current_node_block_id}. New key count: {num_keys_after_insert}")
        else:
            # Leaf node is full, needs splitting.
            # This is a complex operation:
            # 1. Create a new leaf node.
            # 2. Distribute keys/pointers between old and new leaf (approx half-half).
            # 3. Take the median key (or first key of new node) to be pushed up to the parent.
            # 4. Update parent, which might also split, and so on up to the root.
            # If root splits, a new root is created, and tree height increases.
            print(f"Leaf node {current_node_block_id} is full (keys: {num_keys_after_insert}). Split required.")
            # Haomin Wang: SPLIT LOGIC IS A MAJOR PART AND IS NOT IMPLEMENTED HERE.
            # This would involve a call like:
            # promoted_key, new_child_block_id = self._split_leaf_node(current_node_block_id, leaf_node_buf, keys_in_leaf, ptrs_in_leaf)
            # self._insert_into_parent(path_to_leaf_exclusive_of_current, promoted_key, new_child_block_id)
            # For now, we'll just indicate a split is needed.
            # A proper implementation would handle this recursively or iteratively.
            pass # Placeholder for split logic

    # Haomin Wang: Placeholder for leaf split
    def _split_leaf_node(self, old_leaf_block_id, old_leaf_buf, full_keys_list, full_ptrs_list):
        # ... (Detailed logic for splitting a leaf node) ...
        # 1. Allocate new_leaf_block_id.
        # 2. Create new_leaf_buf.
        # 3. Split keys/ptrs: ~half in old_leaf, ~half in new_leaf.
        #    - E.g., first ceil(N/2) in old, rest in new.
        # 4. Update num_keys in both node buffers.
        # 5. Update next_leaf_ptr for old_leaf_buf to point to new_leaf_block_id.
        #    new_leaf_buf's next_leaf_ptr takes old_leaf_buf's original next.
        # 6. Write both modified buffers to disk.
        # 7. Return the key to be promoted (typically the first key of the new_leaf)
        #    and the new_leaf_block_id.
        print(f"Error: _split_leaf_node not fully implemented.")
        return None, None # promoted_key, new_right_child_block_id

    # Haomin Wang: Placeholder for inserting a key into a parent internal node after a child split
    def _insert_into_parent(self, path, key_to_promote, new_right_child_block_id):
        # ... (Detailed logic for inserting into parent, potentially splitting parent) ...
        # 1. If path is empty, means the root was split. Create new root. Increase tree height.
        # 2. Else, get parent_block_id from path. Read parent_node_buf.
        # 3. Insert key_to_promote and new_right_child_block_id into parent's keys/pointers.
        #    (The pointer to the left child of key_to_promote is already in the parent).
        # 4. If parent is full, split parent:
        #    - new_parent_key_to_promote, new_parent_right_child_block_id = _split_internal_node(...)
        #    - _insert_into_parent(path_to_grandparent, new_parent_key_to_promote, new_parent_right_child_block_id)
        # 5. Else (parent not full), write updated parent_node_buf to disk.
        print(f"Error: _insert_into_parent not fully implemented.")
        pass

    # Haomin Wang: Search for a key, returns list of (data_block_id, data_offset_in_block)
    def search_key(self, key_value):
        print(f"Searching for key: {key_value}")
        if not self.has_root:
            print("Search failed: B-tree is empty.")
            return []

        formatted_key = self._format_key(key_value)

        current_node_block_id = self.root_node_ptr

        # Traverse down the tree
        for _ in range(self.num_of_levels): # Max depth traversal
            node_buf = self._read_node_block(current_node_block_id)
            if not node_buf:
                print(f"Search Error: Failed to read node block {current_node_block_id}")
                return []

            _, node_type, num_keys = struct.unpack_from(NODE_HEADER_FORMAT, node_buf, 0)

            if node_type == LEAF_NODE_TYPE:
                # We are at a leaf node, search for the key here
                results = []
                current_offset = NODE_HEADER_SIZE
                for i in range(num_keys):
                    key_in_leaf_bytes = struct.unpack_from(KEY_FORMAT, node_buf, current_offset)[0]
                    current_offset += KEY_SIZE

                    ptr_block_id, ptr_offset = struct.unpack_from(LEAF_POINTER_FORMAT, node_buf, current_offset)
                    current_offset += LEAF_POINTER_SIZE

                    if key_in_leaf_bytes == formatted_key:
                        results.append((ptr_block_id, ptr_offset))
                    elif key_in_leaf_bytes > formatted_key and results:
                        # Keys are sorted, if current key is greater and we found matches, no more matches possible
                        break
                    elif key_in_leaf_bytes > formatted_key and not results:
                        # Key not found (passed where it should be)
                        break

                if results: print(f"Key found. Data pointers: {results}")
                else: print("Key not found in leaf.")
                return results

            elif node_type == INTERNAL_NODE_TYPE:
                # Internal node, find which child pointer to follow
                current_node_block_id = self._find_child_ptr_in_internal_node(formatted_key, node_buf)
                if current_node_block_id == SPECIAL_INDEX_BLOCK_PTR:
                    print("Search Error: Invalid child pointer in internal node.")
                    return []
            else:
                print(f"Search Error: Unknown node type {node_type} encountered.")
                return []

        print("Search Error: Reached max tree depth without finding a leaf node (should not happen in a consistent B-tree).")
        return []


# the following is to test
if __name__ == '__main__':
    # Haomin Wang: Initialize common_db.BLOCK_SIZE if it's not set by other means during test
    if not hasattr(common_db, 'BLOCK_SIZE') or not common_db.BLOCK_SIZE:
        common_db.BLOCK_SIZE = 4096 # Default for testing

    print(f"Testing index_db.py with BLOCK_SIZE = {common_db.BLOCK_SIZE}")

    # Clean up old index file for fresh test
    test_index_file = "test_table.ind"
    if os.path.exists(test_index_file):
        os.remove(test_index_file)

    index_obj = Index('test_table') # Create/open index for 'test_table'

    # Test insertions
    # (key_value, data_block_id, data_offset_in_block)
    test_data = [
        ("apple", 1, 101),
        ("banana", 1, 102),
        ("cherry", 2, 201),
        ("date", 2, 202),
        ("elderberry", 3, 301),
        ("fig", 3, 302),
        ("grape", 4, 401),
    ]

    # Haomin Wang: Reduced MAX_NUM_OF_KEYS globally, or better use MAX_LEAF_KEYS from calculation
    # If MAX_LEAF_KEYS is small (e.g., 2 or 3), splits will happen sooner.
    # For this test, MAX_LEAF_KEYS depends on BLOCK_SIZE and element sizes.
    # If BLOCK_SIZE=4096, KEY_SIZE=10, LEAF_POINTER_SIZE=8. LEAF_ENTRY_SIZE=18.
    # NODE_HEADER_SIZE=12, NEXT_LEAF_BLOCK_ID_SIZE=4
    # MAX_LEAF_KEYS = (4096 - 12 - 4) / 18 = 4080 / 18 = 226.
    # To test splits easily, we need a much smaller BLOCK_SIZE or many more keys,
    # or manually set MAX_LEAF_KEYS to a small number for testing.
    # Let's assume MAX_LEAF_KEYS will be dynamically calculated, for now it might be large.

    for data in test_data:
        index_obj.insert_index_entry(data[0], data[1], data[2])

    print("\n--- Search Tests ---")
    search_keys = ["banana", "date", "fig", "avocado", "grape"]
    for skey in search_keys:
        results = index_obj.search_key(skey)
        if not results:
            print(f"Search result for '{skey}': Not Found")
        # else: results already printed by search_key

    del index_obj # Test destructor and saving meta

    # Re-open and test if data persists
    print("\n--- Re-opening index to test persistence ---")
    index_obj_reopened = Index('test_table')
    results_reopened = index_obj_reopened.search_key("cherry")
    if not results_reopened:
         print(f"Search result for 'cherry' after reopen: Not Found")

    del index_obj_reopened
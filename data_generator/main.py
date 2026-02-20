import csv
import random
import os
from datetime import datetime, timedelta


def generate_transaction_batch(num_records, batch_timestamp, existing_ids, next_id, last_change_timestamps):
    """
    Generate a single batch of transaction records.
    
    Args:
        num_records: Number of transaction records in this batch
        batch_timestamp: Base timestamp for this batch
        existing_ids: Set of existing transaction IDs that can be updated/deleted
        next_id: Next available ID for new insertions
        last_change_timestamps: Dictionary mapping ID to last change timestamp
    
    Returns:
        Tuple of (list of transaction dictionaries, updated existing_ids set, updated next_id, updated last_change_timestamps dict)
    """
    # Define possible values
    transaction_types = ['CARD', 'MANUAL', 'GOOGLEPAY', 'APPLEPAY']
    states = ['PENDING', 'SETTLED', 'CANCELLED']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD', 'AUD', 'CHF', 'CNY', 'INR', 'BRL']
    
    transactions = []
    updated_existing_ids = existing_ids.copy()
    updated_last_change_timestamps = last_change_timestamps.copy()
    
    # Generate records with timestamps within a small window around batch_timestamp
    time_window = timedelta(hours=1)  # All records in batch within 1 hour window
    
    for _ in range(num_records):
        # Generate random timestamp within the batch window
        random_offset = random.randint(0, int(time_window.total_seconds()))
        transaction_timestamp = batch_timestamp + timedelta(seconds=random_offset)
        
        # Determine operation type
        # If we have existing IDs, we can do I, U, or D
        # Otherwise, we can only do I (Insert)
        if existing_ids and random.random() < 0.6:  # 60% chance of U or D if IDs exist
            if random.random() < 0.7:  # 70% of those are Updates
                op = 'U'
                transaction_id = random.choice(list(existing_ids))
            else:  # 30% are Deletes
                op = 'D'
                transaction_id = random.choice(list(existing_ids))
                updated_existing_ids.discard(transaction_id)  # Remove from existing after delete
        else:  # Insert new record
            op = 'I'
            transaction_id = next_id
            updated_existing_ids.add(transaction_id)
            next_id += 1
        
        # Determine last_change timestamp
        if op == 'I':
            # For inserts, last_change is the same as transaction_created_timestamp
            last_change = transaction_timestamp
        else:
            # For updates/deletes, ensure last_change is after the previous one
            previous_last_change = updated_last_change_timestamps.get(transaction_id, batch_timestamp - timedelta(days=1))
            # Ensure the new last_change is at least 1 second after the previous one
            min_last_change = previous_last_change + timedelta(seconds=1)
            # Use the later of transaction_timestamp or min_last_change
            last_change = max(transaction_timestamp, min_last_change)
        
        # Update the last_change_timestamps dictionary
        updated_last_change_timestamps[transaction_id] = last_change
        
        # Generate transaction data
        transaction = {
            'OP': op,
            'ID': transaction_id,
            'TYPE': random.choice(transaction_types),
            'STATE': random.choice(states),
            'Currency': random.choice(currencies),
            'amount': random.randint(100, 1000000),  # Amount in cents (1.00 to 10000.00)
            'transaction_created_timestamp': transaction_timestamp.strftime('%Y-%m-%d %H:%M:%S'),
            'last_change': last_change.strftime('%Y-%m-%d %H:%M:%S')
        }
        transactions.append(transaction)
    
    return transactions, updated_existing_ids, next_id, updated_last_change_timestamps


def generate_cdc_batches(num_batches=10, records_per_batch=100, output_dir="output"):
    """
    Generate multiple CDC batch CSV files for Transactions.
    
    Args:
        num_batches: Number of CDC batches to generate (default: 10)
        records_per_batch: Number of records per batch (default: 100)
        output_dir: Output directory for CSV files (default: output)
    """
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate base timestamps for batches (spread over the last 30 days)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    fieldnames = ['OP', 'ID', 'TYPE', 'STATE', 'Currency', 'amount', 'transaction_created_timestamp', 'last_change']
    
    # Track existing IDs across batches for Point-In-Time processing
    existing_ids = set()
    next_id = 1  # Start IDs from 1
    # Track last change timestamp for each ID to ensure chronological order
    last_change_timestamps = {}
    
    for batch_num in range(1, num_batches + 1):
        # Generate batch timestamp (spread batches over time)
        batch_offset = (batch_num - 1) * (end_date - start_date).total_seconds() / num_batches
        batch_timestamp = start_date + timedelta(seconds=batch_offset)
        
        # Generate transactions for this batch
        transactions, existing_ids, next_id, last_change_timestamps = generate_transaction_batch(
            records_per_batch, batch_timestamp, existing_ids, next_id, last_change_timestamps
        )
        
        # Create batch filename with timestamp
        batch_filename = f"batch_{batch_num:04d}_{batch_timestamp.strftime('%Y%m%d_%H%M%S')}.csv"
        output_file = os.path.join(output_dir, batch_filename)
        
        # Write batch to CSV
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(transactions)
        
        print(f"Generated batch {batch_num}/{num_batches}: {records_per_batch} records in {output_file}")
    
    print(f"\nTotal: Generated {num_batches} CDC batches with {records_per_batch} records each in '{output_dir}/' directory")


def main():
    # Generate 10 CDC batches with 100 records each by default
    # You can modify these parameters as needed
    generate_cdc_batches(num_batches=10, records_per_batch=100, output_dir="output")


if __name__ == "__main__":
    main()

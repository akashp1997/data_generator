import hashlib
import argparse
import multiprocessing
from numpy import random
import os
from time import perf_counter, sleep

def generate_data(process_no, file_size, **kwargs):
    print(os.path.join(kwargs['path'], f'blr_data_{process_no}.raw'))
    with open(os.path.join(kwargs['path'], f'blr_data_{process_no}.raw'), 'wb') as out_file:
        random_state = random.RandomState(kwargs['seed'])
        process_write_speed = kwargs['write_speed'] / kwargs['num_writers']
        while file_size > 0:
            num_megabytes = min(process_write_speed, file_size)
            start_time = perf_counter()
            out_file.write(random_state.bytes(num_megabytes * 1024 * 1024))
            out_file.flush()
            diff_time = perf_counter() - start_time
            if diff_time < 1:
                sleep(1 - diff_time)
            file_size -= num_megabytes
        out_file.close()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    argument_parser = argparse.ArgumentParser()
    argument_parser.add_argument('--produce', action='store_true')
    argument_parser.add_argument('--verify', action='store_true')
    argument_parser.add_argument('--path', default='', help='Path to read/write files to/from')
    argument_parser.add_argument('--num_writers', default=10, help='Number of file writers to use in parallel',
                                 type=int)
    argument_parser.add_argument('--data_size', default=5, help='Maximum data size in GB', type=float)
    argument_parser.add_argument('--max_file_size', default=128, help='File size in MB', type=int)
    argument_parser.add_argument('--write_speed', default=10, help='File write speed in MBPS', type=float)
    argument_parser.add_argument('--seed', default=10, type=int)
    args = vars(argument_parser.parse_args())
    start_time = perf_counter()
    if args['path'] and not os.path.exists(args['path']) or os.path.isfile(args['path']):
        quit(3)
    if args['produce']:
        pool = multiprocessing.Pool(processes=args['num_writers'])
        try:
            file_size_left = args['data_size'] * 1024
            index = 0
            while file_size_left > 0:
                file_size = min(args['max_file_size'], file_size_left)
                pool.apply_async(generate_data, (index, file_size), args)
                file_size_left -= file_size
                index += 1
            pool.close()
        except KeyboardInterrupt:
            pool.terminate()
        finally:
            pool.join()
        write_speed = args['data_size'] / (perf_counter() - start_time)
        print(f"The average write speed achieved was {write_speed} GBPS")
    elif args['verify']:
        num_full_size_files = int((args['data_size'] * 1024) // args['max_file_size'])
        partial_file_size = (args['data_size'] * 1024) % args['max_file_size']

        # Generate full file SHA256
        random_state = random.RandomState(args['seed'])
        full_size_sha256 = hashlib.sha256(random_state.bytes(args['max_file_size'] * 1024 * 1024)).hexdigest()

        # Generate last file's SHA256
        random_state = random.RandomState(args['seed'])
        partial_sha256 = hashlib.sha256(random_state.bytes(partial_file_size * 1024 * 1024)).hexdigest()

        # Verify hash for all but last file
        for index in range(num_full_size_files):
            print(f'blr_data_{index}.raw')
            sha256 = hashlib.sha256(open(os.path.join(args['path'], f'blr_data_{index}.raw'), 'rb').read()).hexdigest()
            assert sha256 == full_size_sha256

        if os.path.exists(os.path.join(args['path'], f'blr_data_{num_full_size_files}.raw')):
            sha256 = hashlib.sha256(
                open(os.path.join(args['path'], f'blr_data_{num_full_size_files}.raw'), 'rb').read()).hexdigest()
            assert sha256 == partial_sha256
        print('All files\' checksum has been verified')
    time_elapsed = perf_counter() - start_time
    print(f"Time elapsed: {time_elapsed}")

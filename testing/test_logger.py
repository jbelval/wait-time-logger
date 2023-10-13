from logger import Logger
from collections import namedtuple
import pytest
import os

LoggerTestCase = namedtuple(
    'LoggerTestCase',
    [
        'description',
        'log_file',
        'data_file'
    ]
)

logger_test_cases = [
    LoggerTestCase(
        'test1',
        'test1.txt',
        'test1.csv'
    ),
    LoggerTestCase(
        'test2',
        'test2.txt',
        'test2.csv'
    )
]

@pytest.fixture(scope='module', autouse=True)
def remove_testing_output():
    if os.path.exists('test_results.txt'):
        os.remove('test_results.txt')

@pytest.mark.parametrize('description, log_file, data_file', logger_test_cases)
def test_logger(description, log_file, data_file):
    # Create logger
    l = Logger(
        log_file=f'{description}_output.txt',
        data_file=f'{description}_output.csv',
    )
    # Remove testing output files if they exist
    if os.path.exists(l.log_file):
        os.remove(l.log_file)
    if os.path.exists(l.data_file):
        os.remove(l.data_file)

    # Grab test log info, and feed it to logger. Ensure only the message gets sent
    with open(log_file, 'r') as f:
        logs = f.readlines()
    for log in logs:
        l.receive(log.split(' - ')[-1])

    # Get output files
    log_output = l.log_file
    data_output = l.data_file

    # Delete logger to ensure all functionality is tested
    del l

    # Testing log file
    msgs = [description]
    incorrect = False
    with open(log_file, 'r') as f:
        input_logs = f.readlines()
        input_logs = [x.split(' - ')[-1].strip() for x in input_logs]
    with open(log_output, 'r') as f:
        output_logs = f.readlines()
        output_logs = [x.split(' - ')[-1].strip() for x in output_logs]

    if len(input_logs) != len(output_logs):
        incorrect = True
        msgs.append('  Log files do not contain same number of rows')
    for i in range(min(len(input_logs), len(output_logs))):
        if input_logs[i] != output_logs[i]:
            incorrect = True
            msgs.append(f'    Log files do not match at row {i}')

    if not incorrect:
        msgs[0] += ' Passed!'
    else:
        msgs[0] += ' Failed :('

    with open('test_results.txt', 'a') as f:
        for l in msgs:
            f.write(f'{l}\n')

    if incorrect:
        raise Exception('Test failed')


if __name__ == '__main__':
    pytest.main(__file__)
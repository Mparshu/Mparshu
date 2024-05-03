
import pytest
import time
import subprocess
from _pytest.runner import runtestprotocol
from _pytest.capture import CaptureFixture
from io import StringIO
import sys
import glob

def pytest_sessionstart(session):
    #Ask user for their email ID
    #email_id = input("Please enter your email ID:")
    #session.config.option.email_id = email_id
    #test_case_results = {}
    session.results = dict()
    session.test_case_results = dict()

@pytest.fixture(scope="session", autouse=True)
def test_results():
    # Setup phase: Initialize the list to store results
    results = []
    yield results # Pass the results list to the test case

    # Teardown phase: You can perform any cleanup here if needed
    # For example, printing the results after all tests have run
    print("Results collected from tests:", results)


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    # execute all other hooks to obtain the report object
    outcome = yield
    result = outcome.get_result()

    if result.when == "call":
        item.session.results[item] = result

def pytest_runtest_protocol(item, nextitem):

    reports = runtestprotocol(item, nextitem=nextitem)
    for report in reports:
        if report.when == 'call':
            test_class = item.cls.__name__ if hasattr(item,'cls')else None
            full_test_case_name = test_class + "::" + item.name
            item.session.test_case_results[full_test_case_name] = report.outcome

    return True

def pytest_sessionfinish(session, exitstatus):
    print('run status code:', exitstatus)
    # Calculate passed and failed tests
    passed_amount = sum(1 for result in session.results.values() if result.passed)
    failed_amount = sum(1 for result in session.results.values() if result.failed)
    print(f'There are {passed_amount} passed and {failed_amount} failed tests')
    #results = session.config.cache.get("test_results", [])
    print(session.results)
    #print("results collected from sessionfinish tests:", results)

    # Define the column widths for neat table
    test_case_width = 80
    outcome_width = 20

    # Print the header
    print(f"{'Test Case':<{test_case_width}} | {'Outcome':<{outcome_width}}")
    print('-' * (test_case_width + outcome_width + 3)) #Adjust the length of the seperator line as needed 

    # Print each test case result
    for test_case, value in session.test_case_results.items():
        print(f"{test_case:<{test_case_width}} | {value:<{outcome_width}}")


    # Calculate execution time
    reporter = session.config.pluginmanager.get_plugin('terminalreporter')
    duration = time.time() - reporter._sessionstarttime
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)
    # Use and f-string to format the output
    execution_time = f"{int(hours):02d}:{(minutes):02d}:{int(seconds):02d} seconds"

# ============================================================================
#
# File    : test_NYT.py
# Date    : 2024/11/17
# (c)     : Michaël Abergel - Alfred Quitman - Emmanuel Bompard
# Object  : API test script (may be run either manually or from a container)
# Version : 0.2.0
#
# ============================================================================

## Imports
#
import os
import requests
from requests.auth import HTTPBasicAuth
import json
import yaml

from datetime import datetime

# ---------------------------------------------------------------------

# Utilities

def my_now() -> str:
    return datetime.now().strftime('[%Y-%m-%d:%H:%M:%S]')

def str_log(p_str: str, p_label: str, p_msg: str) -> str:
    return f"{p_str}{my_now()} - {p_label}: {p_msg}\n"
    
def make_url(p_d_endpoint: dict) -> str:
    return "{}://{}:{}{}".format(
            p_d_endpoint["api_protocol"],
            p_d_endpoint["api_address"],
            p_d_endpoint["api_port"],
            p_d_endpoint["endpoint"],
        )

# ---------------------------------------------------------------------

# Main requests functions

def auth_request_get(p_url: str, p_d_test: dict):
    """
    Run the request for authentification test (GET).
    
    Parameters :
      - p_url : the endpoint URL
      - p_d_test : the test dictionary
    
    Returns :
      - the reply from the endpoint
    """
    # Get the authentication object
    _auth_basic = HTTPBasicAuth(
        p_d_test["username"],
        p_d_test["password"],
    )

    # Run the request :
    if "params" in p_d_test:
        _req_res = requests.get(
            url = p_url,
            headers = _headers,
            auth = _auth_basic,
            data = json.dumps(p_d_test["params"]),
        )
    else:
        _req_res = requests.get(
            url = p_url,
            auth = _auth_basic,
        )

    return _req_res


def auth_request_post(p_url: str, p_d_test: dict):
    """
    Run the request for authentification test (POST).
    
    Parameters :
      - p_url : the endpoint URL
      - p_d_test : the test dictionary
      - p_params : parameters dictionary
    
    Returns :
      - the reply from the endpoint
    """
    # Get the authentication object
    _auth_basic = HTTPBasicAuth(
        p_d_test["username"],
        p_d_test["password"],
    )

    # Specify the headers
    _headers = {'Content-Type': 'application/json'}
    
    # Run the request :
    if "params" in p_d_test:
        _req_res = requests.post(
            url = p_url,
            headers = _headers,
            auth = _auth_basic,
            data = json.dumps(p_d_test["params"]),
        )
    else:
        _req_res = requests.post(
            url = p_url,
            headers = _headers,
            auth = _auth_basic,
        )

    return _req_res

def run_test(p_label: str, p_params: dict, *, p_data=False) -> str:
    """
    Runs tests against the DS API.
    Authorization and content tests are based on the same formal test model.
    Authentication test does not require to send some data.
    
    Parameters :
      - p_label : the test label
      - p_params : a dictionary giving all needed parameters to run the
        tests
      - p_data : if True, it means some data has to be sent (the sentence
        to classify)
    
    Returns :
      - an output string to be logged.
    """
    _test_name = p_label
    
    # Output initialisation
    _output = str_log("", _test_name, "Start")
    
    # We loop thru the batches to run :
    for _batch_k, _batch_v in p_params.items():
        _output = str_log(_output, _test_name, f" >> Batch: {_batch_k}")
        
        # Get the endpoint URL
        _url = make_url(_batch_v["api_server"])
        _output = str_log(_output, _test_name, f"    Endpoint: {_url}")

        # Run this batch tests
        for _test_k, _test_v in _batch_v["tests"].items():
            _output = str_log(_output, _test_name, f"    >> Test {_test_k}")

            _score_ok = None
            # Run the request :
            if p_data:
                # Content/Authorization request :
                if _batch_v["api_server"]["method"] == "post":
                    _r = auth_request_post(_url, _test_v)
                else:
                    # "get" method
                    _r = auth_request_get(_url, _test_v)
                
                if (_r.status_code == _test_v['expect']):
                    # Check the expected data
                    if "returns" in _test_v:
                        _expected_data = _test_v["returns"]["value"]
                        _got_data = _r.json()[_test_v["returns"]["var"]]
                        
                        # Check the score is as expected
                        if "test" in _test_v["returns"]:
                            _test_str = _test_v["returns"]["test"]
                            _score_ok = eval(f"""{_got_data} {_test_v["returns"]["test"]} {_expected_data}""")
                        else:
                            _test_str = ""
                            _score_ok = (_expected_data == _got_data)

                        _test_result = "OK" if _score_ok else "KO"
                    else:
                        _score_ok = None
                        _test_result = "OK"
                else:
                    _test_result = "KO"
            else:
                # Authentication request
                _score_ok = None
                if _batch_v["api_server"]["method"] == "post":
                    _r = auth_request_post(_url, _test_v)
                else:
                    # "get" method
                    _r = auth_request_get(_url, _test_v)
                _test_result = "OK" if (_r.status_code == _test_v['expect']) else "KO"
            
            _output = str_log(_output, _test_name, f"       Sent: username: {_test_v['username']}")
            if _score_ok is not None:
                _output = str_log(_output, _test_name, f"             params: {_test_v['params']}")
                _output = str_log(_output, _test_name, f"       Got:  value: {_got_data} (expected: {_test_str}{_expected_data})")
            _output = str_log(_output, _test_name, f"       Code Expected: {_test_v['expect']}")
            _output = str_log(_output, _test_name, f"       Code Received: {_r.status_code}")
            _output = str_log(_output, _test_name, f"       => Result: {_test_result}")

    # End of output
    _output = str_log(_output, _test_name, "End")

    return _output

# ---------------------------------------------------------------------

def test_authentication(p_params: dict):
    """
    Runs authentication tests against the DS API.
    
    Parameters :
      - p_params : a dictionary giving all needed parameters to run the
        tests
    
    Returns :
      - an output string to be logged.
    """
    return run_test("Authentication", p_params)

def test_authorization(p_params: dict):
    """
    Runs authorization tests against the DS API.
    
    Parameters :
      - p_params : a dictionary giving all needed parameters to run the
        tests
    
    Returns :
      - an output string to be logged.
    """
    return run_test("Authorization", p_params)

def test_content(p_params: dict):
    """
    Runs content tests against the DS API.
    
    Parameters :
      - p_params : a dictionary giving all needed parameters to run the
        tests
    
    Returns :
      - an output string to be logged.
    """
    return run_test("Content", p_params, p_data=True)

# ---------------------------------------------------------------------

def main():
    # Loads the parameters file (YAML) into a dictionary
    # (the environment variable HAS to exist)
    with open(os.environ["DS_API_PARAMS"], "r") as _f:
        d_params = yaml.safe_load(_f)

    if "DS_API_TEST_TO_RUN" in os.environ:
        _test = os.environ["DS_API_TEST_TO_RUN"]
    else:
        _test = "all"

    output= ""
    match _test:
        case "AUTHENTICATION":
            if "authentication" in d_params:
                output += test_authentication(d_params["authentication"])
        case "AUTHORIZATION":
            if "authorization" in d_params:
                output += test_authorization(d_params["authorization"])
        case "CONTENT":
            if "content" in d_params:
                output += test_content(d_params["content"])
        case _:
            # Run all tests
            if "authentication" in d_params:
                output += test_authentication(d_params["authentication"])
            if "authorization" in d_params:
                output += test_authorization(d_params["authorization"])
            if "content" in d_params:
                output += test_content(d_params["content"])

    # When requested, write the output in a file (append mode)
    if "DS_API_TEST_LOG" in os.environ:
        with open(os.environ["DS_API_TEST_LOG"], 'a') as file: 
            file.write(output)
    else:
        print(output)

    return

#
if __name__ == "__main__":
    main()

import pytest
from pyarrow import flight
from arrow_flight.server import MyFlightServer

@pytest.fixture
def server():
    return MyFlightServer(("0.0.0.0", 8815))

@pytest.fixture
def client():
    return flight.FlightClient(("localhost", 8815))

def test_do_action(server,client):
    assert server
    assert client.do_action(flight.Action("get_data_1", "dataset_id".encode('utf-8'))) is NotImplementedError
    client.do_action(flight.Action("get_schema", "dataset_id".encode('utf-8')))

def test_subtract(calculator):
    assert calculator.subtract(5, 2) == 3
    assert calculator.subtract(-1, -1) == 0
    assert calculator.subtract(-1, 1) == -2

def test_divide(calculator):
    assert calculator.divide(10, 2) == 5
    assert calculator.divide(5, 2) == 2.5
    with pytest.raises(ValueError, match="Cannot divide by zero."):
        calculator.divide(10, 0)

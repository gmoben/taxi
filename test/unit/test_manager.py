from concurrent.futures import ThreadPoolExecutor
from threading import Thread
import time

from mock import patch, Mock
import pytest


@pytest.skip('flaky')
def test_poll_workers(Manager, Worker):
    with patch.object(Worker, 'get_status', return_value='mock_status') as mock_status:
        manager = Manager()
        worker = Worker()

        t = Thread(target=worker.listen)
        t.start()
        time.sleep(1)

        on_response = Mock()
        response = manager.poll_workers(on_response)

        mock_status.assert_called_once()
        on_response.assert_called_once()
        assert on_response.call_args == (worker.get_status(),)

@pytest.skip('flaky')
def test_publish_work(Manager, Worker):
    with patch.object(Worker, 'on_work') as mock_on_work:
        manager = Manager()
        worker = Worker()

        t = Thread(target=worker.listen)
        t.start()
        time.sleep(1)

        work_data = 'Do ur work'
        manager.publish_work(work_data)

        with ThreadPoolExecutor() as ex:
            def make_sure():
                while not mock_on_work.called:
                    time.sleep(1)
                msg = mock_on_work.call_args
                assert msg[0][0].data == work_data
            future = ex.submit(make_sure)
            future.result()

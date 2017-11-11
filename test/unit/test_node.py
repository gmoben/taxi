from mock import patch


def test_init(Node):
    with patch.object(Node, 'setup') as mock_setup:
        node = Node()
        assert node.connected
        mock_setup.assert_called_once()

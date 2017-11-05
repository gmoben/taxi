from taxi import Worker

_LogWorker = Worker('logging')


class MessageLogger(_LogWorker):

    def setup(self):
        self.subscribe('>', self.on_msg)

    def on_msg(self, msg):
        """ Print truncated message """
        self.log.info(msg.data[:1024].strip())

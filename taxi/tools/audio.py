import base64
import time
import uuid

try:
    import av
except ImportError:
    pass

import structlog

from taxi import Manager, Worker
from taxi.util import subtopic, StringTree


NAMESPACE = 'audiostream'
LOG = structlog.getLogger(__name__)

CMD = StringTree('cmd', ['play'])
AUDIO = StringTree('audio', ['start', 'end', 'chunk'])


def timestamp(struct_time=None):
    """ RTES format timestamps because it's important """
    return time.strftime('%Y-%m-%dT%TZ', struct_time or time.gmtime())


def resample_audio(path, sample_rate=None, sample_format=None, audio_layout=None):
    """
    Open an audio file and generate resampled frames

    :param path: path to the audio file
    :param sample_format: libav audio format
    :param audio_layout: libav audio layout
    :param sample_rate: new sample rate
    :type path: string
    :type sample_format: string
    :type audio_layout: string
    :type sample_rate: int
    """
    log = LOG.bind(path=path,
                   sample_rate=sample_rate,
                   sample_format=sample_format,
                   audio_layout=audio_layout)
    # Get first available audio stream from the file
    container = av.open(path)
    stream = next(s for s in container.streams if s.type == 'audio')
    resampler = None

    stream_details = [stream.rate, stream.format.name, stream.layout.name]
    requested_details = [sample_rate, sample_format, audio_layout]

    # Skip transcoding if it's the format we want
    if not any(requested_details):
        log.warning('Decoding without resampling', path)
    elif stream_details == requested_details:
        log.info('No resampling required')
    else:
        log.debug('Resampling')
        resampler = av.AudioResampler(
            format=av.AudioFormat(sample_format) if sample_format else None,
            layout=av.AudioLayout(audio_layout) if audio_layout else None,
            rate=sample_rate if sample_rate else None
        )
    for packet in container.demux(stream):
        for frame in packet.decode():
            if resampler:
                # Resampler will return None until it has enough data for a new frame
                # XXX: Do you lose the very last bits of audio in this case?
                frame = resampler.resample(frame)
            if frame:
                yield frame


class AudioPlaybackManager(Manager(NAMESPACE)):

    def setup(self):
        self.subscribe(CMD.PLAY, lambda msg: self.publish_work(msg.data))


class AudioPlaybackWorker(Worker(NAMESPACE)):

    def setup(self):
        """
        :param sample_rate: destination sample rate
        :param sample_format: destination libav audio format
        :param audio_layout: destination libav audio layout
        """
        self.sample_rate = 8000
        self.sample_format = 's16p'
        self.audio_layout = 'mono'

    def on_work(self, msg):
        audio_id = str(uuid.uuid4())
        audio_path = str(msg.data)
        self.play_audio(audio_path, audio_id)

    def publish_start(self, audio_id):
        self.publish(subtopic(AUDIO.START, audio_id), timestamp())

    def publish_end(self, audio_id):
        self.publish(subtopic(AUDIO.END, audio_id), timestamp())

    def publish_chunk(self, chunk, audio_id):
        if chunk:
            data = chunk.planes[0].to_bytes()
            data = base64.b64encode(data)
            self.publish(subtopic(AUDIO.CHUNK, audio_id), data)

    def play_audio(self, path, audio_id):
        """
        Open an audio file, transcode it and send it in chunks

        Supports any file type that libav supports.

        :param client: NATSClient instance
        :param path: path to the audio file.
        :param chunk_size: number of samples to send per message.
        :param audio_id: audio_id for a the call (Default: uuid.uuid4())
        :type path: string
        :type chunk_size: int
        :type audio_id: string
        :type sample_rate: int
        :type sample_format: string
        :type audio_layout: string
        """

        try:
            fifo = av.AudioFifo()

            # Write each transcoded frame to an AudioFifo, then
            # create a chain of tasks to send chunks of data
            for frame in resample_audio(path, self.sample_rate, self.sample_format, self.audio_layout):
                fifo.write(frame)

            chunk_size = getattr(self, 'chunk_size', self.sample_rate)
            chunk_rate = getattr(self, 'chunk_rate', float(chunk_size) / self.sample_rate)

            self.publish_start(audio_id)

            chunk = fifo.read(chunk_size)
            while chunk:
                time.sleep(chunk_rate)
                self.publish_chunk(chunk, audio_id)
                chunk = fifo.read(chunk_size)

            # Queue the last bit of audio regardless of the chunk size
            chunk = fifo.read(0)
            time.sleep(chunk_rate)
            self.publish_chunk(chunk, audio_id)

            self.publish_end(audio_id)
        except:
            LOG.exception('Unhandled exception during play_audio')

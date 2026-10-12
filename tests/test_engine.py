import unittest
import asyncio
import io
import tarfile
from volmigrate.engine.streaming import StreamingTransferEngine
from volmigrate.engine.validator import TransferValidator
from volmigrate.models.enums import ValidationPolicy


class TestEngine(unittest.TestCase):
    def test_streaming_transfer_metrics(self):
        async def _run():
            # Generate dummy in-memory tar stream
            tar_buf = io.BytesIO()
            with tarfile.open(fileobj=tar_buf, mode="w") as tar:
                data = b"Hello from volmigrate streaming test!"
                ti = tarfile.TarInfo(name="test.txt")
                ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))

            tar_bytes = tar_buf.getvalue()

            async def _generator():
                chunk_size = 64
                for i in range(0, len(tar_bytes), chunk_size):
                    yield tar_bytes[i : i + chunk_size]

            progress_reports = []

            async def _callback(transferred, bps, checksum):
                progress_reports.append((transferred, bps, checksum))

            engine = StreamingTransferEngine(progress_callback=_callback)

            # Iterate and consume stream
            total_read = 0
            async for chunk in _generator():
                total_read += len(chunk)
                engine.transferred_bytes += len(chunk)
                engine.hasher.update(chunk)

            self.assertEqual(total_read, len(tar_bytes))
            self.assertEqual(engine.transferred_bytes, len(tar_bytes))
            self.assertEqual(len(engine.hasher.hexdigest()), 64)

        asyncio.run(_run())


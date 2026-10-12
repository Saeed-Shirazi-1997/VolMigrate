"""Volmigrate Worker Daemon process."""

import asyncio
import logging
import signal
import sys
from volmigrate.core.config import get_settings
from volmigrate.core.logging import setup_logging
from volmigrate.core.database import init_db, get_db_context
from volmigrate.worker.lease_manager import LeaseManager
from volmigrate.worker.executor import MigrationExecutor

logger = logging.getLogger("volmigrate.worker")


class WorkerDaemon:
    def __init__(self):
        self.settings = get_settings()
        self.lease_manager = LeaseManager(worker_id=self.settings.WORKER_ID)
        self.executor = MigrationExecutor(self.lease_manager)
        self._stop_event = asyncio.Event()

    def stop(self):
        logger.info("Stopping worker daemon %s...", self.settings.WORKER_ID)
        self._stop_event.set()

    async def run(self):
        logger.info("Starting Volmigrate Worker: %s", self.settings.WORKER_ID)
        await init_db()

        while not self._stop_event.is_set():
            try:
                async with get_db_context() as session:
                    job = await self.lease_manager.claim_next_job(session)

                if job:
                    logger.info("Claimed job %s. Beginning execution...", job.id)
                    await self.executor.execute(job.id)
                else:
                    try:
                        await asyncio.wait_for(
                            self._stop_event.wait(),
                            timeout=self.settings.WORKER_POLL_INTERVAL_SECONDS,
                        )
                    except asyncio.TimeoutError:
                        pass
            except Exception as e:
                logger.error("Error in worker polling loop: %s", e, exc_info=True)
                await asyncio.sleep(2.0)

        logger.info("Worker %s shutdown completed.", self.settings.WORKER_ID)


def main():
    setup_logging()
    daemon = WorkerDaemon()

    def _signal_handler():
        daemon.stop()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass  # Windows fallback

    try:
        loop.run_until_complete(daemon.run())
    finally:
        loop.close()


if __name__ == "__main__":
    main()

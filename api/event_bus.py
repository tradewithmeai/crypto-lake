"""
Thread-safe publish-subscribe event bus for distributing real-time data
from the collector to API WebSocket clients.
"""

import asyncio
import threading
from collections import defaultdict
from typing import Any, Dict, List


class EventBus:
    """
    Thread-safe pub/sub event bus.

    The collector thread calls publish() synchronously.
    WebSocket handlers call subscribe()/unsubscribe() and
    await events from the returned queue.
    """

    def __init__(self, max_queue_size: int = 1000):
        self._lock = threading.Lock()
        self._subscribers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._max_queue_size = max_queue_size
        self._published = 0
        self._dropped = 0

    def publish(self, channel: str, event: Dict[str, Any]) -> None:
        """Called from collector thread (sync). Distributes to all subscriber queues."""
        with self._lock:
            self._published += 1
            for q in self._subscribers.get(channel, []):
                try:
                    q.put_nowait(event)
                except asyncio.QueueFull:
                    # Drop oldest to prevent backpressure on collector
                    try:
                        q.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                    try:
                        q.put_nowait(event)
                    except asyncio.QueueFull:
                        pass
                    self._dropped += 1

    def subscribe(self, channel: str) -> asyncio.Queue:
        """Subscribe to a channel. Returns an asyncio.Queue to consume from."""
        q: asyncio.Queue = asyncio.Queue(maxsize=self._max_queue_size)
        with self._lock:
            self._subscribers[channel].append(q)
        return q

    def unsubscribe(self, channel: str, q: asyncio.Queue) -> None:
        """Remove a subscriber queue."""
        with self._lock:
            try:
                self._subscribers[channel].remove(q)
            except ValueError:
                pass

    @property
    def subscriber_count(self) -> int:
        with self._lock:
            return sum(len(qs) for qs in self._subscribers.values())

    @property
    def stats(self) -> Dict[str, Any]:
        return {
            "published": self._published,
            "dropped": self._dropped,
            "subscribers": self.subscriber_count,
        }

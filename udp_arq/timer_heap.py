import heapq
import itertools
from typing import Any, Dict, List, Optional, Tuple

class TimerHeap:
    """A priority queue for managing timeouts without shared mutable state."""
    
    def __init__(self):
        self._heap: List[Tuple[float, int, Any]] = []
        self._key_to_deadline: Dict[Any, float] = {}
        self._counter = itertools.count()
        
    def schedule(self, key: Any, deadline: float) -> None:
        """Schedule or reschedule a key with a new deadline."""
        self._key_to_deadline[key] = deadline
        heapq.heappush(self._heap, (deadline, next(self._counter), key))
        
    def cancel(self, key: Any) -> None:
        """Cancel a scheduled key."""
        self._key_to_deadline.pop(key, None)
        
    def nearest(self) -> Optional[float]:
        """Return the deadline of the soonest valid timer, or None."""
        while self._heap:
            deadline, _, key = self._heap[0]
            if key in self._key_to_deadline and self._key_to_deadline[key] == deadline:
                return deadline
            heapq.heappop(self._heap)
        return None
        
    def pop_due(self, now: float) -> List[Any]:
        """Return all keys whose deadlines are <= now."""
        due_keys = []
        while self._heap:
            deadline, _, key = self._heap[0]
            
            # Skip invalid entries
            if key not in self._key_to_deadline or self._key_to_deadline[key] != deadline:
                heapq.heappop(self._heap)
                continue
                
            if deadline <= now:
                heapq.heappop(self._heap)
                del self._key_to_deadline[key]
                due_keys.append(key)
            else:
                break
                
        return due_keys
        
    def __len__(self) -> int:
        return len(self._key_to_deadline)

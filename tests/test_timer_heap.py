import pytest
from udp_arq.timer_heap import TimerHeap

def test_timer_heap_schedule_pop_ordering():
    th = TimerHeap()
    th.schedule("a", 10.0)
    th.schedule("c", 30.0)
    th.schedule("b", 20.0)
    
    assert len(th) == 3
    assert th.nearest() == 10.0
    
    # Pop at t=15
    due = th.pop_due(15.0)
    assert due == ["a"]
    assert len(th) == 2
    assert th.nearest() == 20.0
    
    # Pop at t=35
    due = th.pop_due(35.0)
    assert due == ["b", "c"]
    assert len(th) == 0
    assert th.nearest() is None

def test_timer_heap_cancel():
    th = TimerHeap()
    th.schedule("a", 10.0)
    th.schedule("b", 20.0)
    
    th.cancel("a")
    assert len(th) == 1
    assert th.nearest() == 20.0
    
    due = th.pop_due(25.0)
    assert due == ["b"]
    assert len(th) == 0

def test_timer_heap_reschedule():
    th = TimerHeap()
    th.schedule("a", 10.0)
    th.schedule("b", 20.0)
    
    # Reschedule a to be later
    th.schedule("a", 30.0)
    assert len(th) == 2
    assert th.nearest() == 20.0
    
    due = th.pop_due(25.0)
    assert due == ["b"]
    
    due = th.pop_due(35.0)
    assert due == ["a"]

def test_timer_heap_empty_behavior():
    th = TimerHeap()
    assert len(th) == 0
    assert th.nearest() is None
    assert th.pop_due(100.0) == []

def test_timer_heap_duplicate_keys_same_time():
    th = TimerHeap()
    th.schedule("a", 10.0)
    th.schedule("a", 10.0) # reschedule same key same time
    
    assert len(th) == 1
    assert th.nearest() == 10.0
    
    due = th.pop_due(15.0)
    assert due == ["a"]
    assert len(th) == 0

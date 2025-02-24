# src/task_queue/partitioned_manager.py

import asyncio
from typing import List, Optional, Dict
from datetime import datetime
import random

from .models import Task, TaskStatus
from .exceptions import QueueError, QueueFullError, TaskNotFoundError

class PartitionedQueueManager:
    """
    A partitioned queue manager that reduces contention by splitting the queue
    into multiple partitions based on task ID hash.
    """
    def __init__(self, partition_count: int = 16, partition_size: int = 1000):
        self.partition_count = partition_count
        self.partition_size = partition_size
        self.partitions = [
            asyncio.Queue(maxsize=partition_size) 
            for _ in range(partition_count)
        ]
        self.task_maps = [{} for _ in range(partition_count)]
        self.locks = [asyncio.Lock() for _ in range(partition_count)]

    def _get_partition(self, task_id: str) -> int:
        """Determine partition based on task ID hash"""
        return hash(task_id) % self.partition_count

    async def enqueue(self, task: Task) -> Task:
        """Enqueue task to appropriate partition"""
        partition_idx = self._get_partition(task.task_id)
        
        async with self.locks[partition_idx]:
            try:
                if task.task_id in self.task_maps[partition_idx]:
                    raise QueueError(f"Task {task.task_id} already exists")
                
                await self.partitions[partition_idx].put(task.task_id)
                self.task_maps[partition_idx][task.task_id] = task
                return task
                
            except asyncio.QueueFull:
                raise QueueFullError(f"Partition {partition_idx} is full")

    async def dequeue(self) -> Optional[Task]:
        """Try to dequeue from any non-empty partition"""
        # Try partitions in random order to prevent starvation
        partition_order = list(range(self.partition_count))
        random.shuffle(partition_order)
        
        for partition_idx in partition_order:
            try:
                async with self.locks[partition_idx]:
                    if not self.partitions[partition_idx].empty():
                        task_id = await self.partitions[partition_idx].get()
                        task = self.task_maps[partition_idx].get(task_id)
                        
                        if task:
                            task.status = TaskStatus.PROCESSING
                            task.started_at = datetime.utcnow()
                            return task
                            
            except Exception as e:
                logger.error(f"Error dequeuing from partition {partition_idx}: {e}")
                
        return None

    async def get_tasks_page(
        self, 
        cursor: Optional[str] = None, 
        limit: int = 100
    ) -> tuple[List[Task], Optional[str]]:
        """Get paginated tasks across all partitions"""
        all_tasks = []
        for task_map in self.task_maps:
            all_tasks.extend(task_map.values())
            
        # Sort by task ID for consistent ordering
        all_tasks.sort(key=lambda x: x.task_id)
        
        # Find cursor position
        start_idx = 0
        if cursor:
            for idx, task in enumerate(all_tasks):
                if task.task_id == cursor:
                    start_idx = idx + 1
                    break
                    
        # Get page
        end_idx = min(start_idx + limit, len(all_tasks))
        tasks_page = all_tasks[start_idx:end_idx]
        
        # Return next cursor if there are more items
        next_cursor = None
        if end_idx < len(all_tasks):
            next_cursor = all_tasks[end_idx].task_id
            
        return tasks_page, next_cursor
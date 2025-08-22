import asyncio
import concurrent.futures
from typing import Optional, Callable, Any
from .validator_factory import validate_code
from .validation_report import ValidationReport


class AsyncValidator:
    """Provides async validation capabilities for non-blocking UX"""
    
    def __init__(self, max_workers: int = 4):
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers)
    
    async def validate_async(
        self, 
        code: str, 
        engine_type: str, 
        original_sql: str = "",
        use_cache: bool = True
    ) -> ValidationReport:
        """Validate code asynchronously"""
        loop = asyncio.get_event_loop()
        
        # Run validation in thread pool to avoid blocking
        result = await loop.run_in_executor(
            self.executor,
            validate_code,
            code,
            engine_type,
            original_sql,
            use_cache
        )
        
        return result
    
    async def validate_batch_async(
        self,
        validation_requests: list,
        progress_callback: Optional[Callable[[int, int], None]] = None
    ) -> list:
        """Validate multiple code samples asynchronously"""
        tasks = []
        
        for i, request in enumerate(validation_requests):
            task = self.validate_async(**request)
            tasks.append(task)
        
        results = []
        completed = 0
        
        # Process tasks as they complete
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            
            if progress_callback:
                progress_callback(completed, len(tasks))
        
        return results
    
    def validate_with_callback(
        self,
        code: str,
        engine_type: str,
        original_sql: str = "",
        callback: Callable[[ValidationReport], None] = None,
        error_callback: Callable[[Exception], None] = None
    ):
        """Validate code and call callback when complete (non-blocking)"""
        def _validate_and_callback():
            try:
                result = validate_code(code, engine_type, original_sql)
                if callback:
                    callback(result)
            except Exception as e:
                if error_callback:
                    error_callback(e)
        
        # Submit to thread pool
        future = self.executor.submit(_validate_and_callback)
        return future
    
    def shutdown(self):
        """Shutdown the async validator"""
        self.executor.shutdown(wait=True)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.shutdown()


# Example usage functions
async def validate_pipeline_async(pipeline, callback=None):
    """Async validation for Pipeline objects"""
    if not pipeline.validate_code or pipeline.engine == "mojo":
        return None
    
    async_validator = AsyncValidator()
    try:
        code = pipeline.gen_last_pipeline(pipeline.parsed_json_whole_query)
        result = await async_validator.validate_async(
            code, 
            pipeline.engine, 
            pipeline.original_query
        )
        
        if callback:
            callback(result)
        
        return result
    finally:
        async_validator.shutdown()


def validate_with_progress(validation_requests, progress_callback=None):
    """Convenience function for batch validation with progress"""
    async def _run_batch():
        async_validator = AsyncValidator()
        try:
            return await async_validator.validate_batch_async(
                validation_requests, 
                progress_callback
            )
        finally:
            async_validator.shutdown()
    
    return asyncio.run(_run_batch())
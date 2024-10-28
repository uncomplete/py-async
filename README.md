# py-async
Simple Python asyncio demo.

## Installation

```$> pip install .```

## Design

Library for coordinating asynchronous tasks and io between them.  
Streams can read and write from multiple different file types.  
Workers can be made to do just about anything - but are made for asyncio tasks (such as calling an API).  

## Example: Batch Inference

```python
await MyApiWorker()
    .input(InputStream('test_data.json'))
    .output(OutputStream('s3://my_bucket/output_data.csv'))
    .run(10)
```

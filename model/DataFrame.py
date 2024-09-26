import uuid
import pyarrow as pa

SCHEMA=pa.schema([
pa.field('text', pa.string()),
pa.field('image', pa.binary()),
pa.field('binary', pa.binary())],
)

class DataFrame:

    def __init__(self, **kwargs):
        self.id=uuid.uuid4()
        self.schema=kwargs.get('schema',None)
        self.nbytes=kwargs.get('nbytes',None)
        self.data=kwargs.get('data',None)


    def concat(self,obj):
        self.data=pa.concat_tables([self.data,obj]) if self.data else obj

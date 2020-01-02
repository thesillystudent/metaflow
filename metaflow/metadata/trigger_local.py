import glob
import json
import os
import time

from metaflow.metaflow_config import DATASTORE_LOCAL_DIR
from .local import LocalMetadataProvider


class TriggerLocalMetadataProvider(LocalMetadataProvider):
    TYPE = 'local-trigger'

    def __init__(self, environment, flow, event_logger):
        super(LocalMetadataProvider, self).__init__(environment, flow, event_logger)

    def new_task_id(self, run_id, step_name, tags=[], sys_tags=[]):
        with diskcache.Cache(CACHE_DIRECTORY) as reference:
            nti = reference.get(run_id)
            if nti is None:
                nti = 0
                reference.add(run_id, nti)
            else:
                reference.incr(run_id)
        self._task_id_seq = nti
        task_id = str(self._task_id_seq)
        self._new_task(run_id, step_name, task_id, tags, sys_tags)
        return task_id

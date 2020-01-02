from .task import MetaflowTask
from .metaflow_config import DEFAULT_DATASTORE, DEFAULT_METADATA
from .datastore.local import LocalDataStore
from .metadata.local import LocalMetadataProvider
from .environment import MetaflowEnvironment
from . import decorators
from .graph import FlowGraph
import logging
from .util import decompress_list
from . import parameters
from .exception import MetaflowInternalError


def metadata_factory(mtype):
    if mtype == "local":
        return LocalMetadataProvider

def datastore_factory(dtype):
    if dtype == "local":
        return LocalDataStore

def flow_factory(flow):
    if flow == "TestLinearFlow":
        from .test_linear_flow import TestLinearFlow
        return TestLinearFlow(use_cli=False)
    elif flow == "TestForkJoinFlow":
        from .test_fork_join import TestForkJoinFlow
        return TestForkJoinFlow(use_cli=False)
    elif flow == "TestForeachJoinFlow":
        from .test_foreach_join import TestForeachJoinFlow
        return TestForeachJoinFlow(use_cli=False)
    elif flow == "TestParameterFlow":
        from .test_parameter import TestParameterFlow
        return TestParameterFlow(use_cli=False)
    elif flow == "TestForkJoin2":
        from .test_flows import TestForkJoin2
        return TestForkJoin2(use_cli=False)
    elif flow == "HelloFlow":
        from .flows.hello_world import HelloFlow
        return HelloFlow(use_cli=False)

class Runner(object):

    def __init__(
        self,
        flow=None,
        metadata=None,
        environment=None,
        datastore=None,
        datastore_root=None,
        # # event_logger=None,
        # # monitor=None,
        step_name=None,
        tags=None,
        run_id=None,
        task_id=None,
        input_paths=None,
        split_index=None,
        user_namespace=None,
        # retry_count=None,
        max_user_code_retries=None,
        clone_only=None,
        clone_run_id=None,
        clone_steps=None,
        join_type=None,
        **kwargs
        ):
        # Unknown params. Clean later
        self._entrypoint = None
        self._clone_run_id = clone_run_id
        self._origin_ds_set = None
        self._is_cloned = {}
        self._split_index = split_index
        self._join_type = join_type


        self._flow = flow_factory(flow)
        self.version = "0.0.1"
        self._graph = FlowGraph(self._flow.__class__)
        self._logger = logging.getLogger(__name__)
        self._environment = MetaflowEnvironment(self._flow)
        self._metadata = metadata_factory(metadata)(
            self._environment,
            self._flow,
            self._logger
            # None
        )
        self._datastore = datastore_factory(datastore)

        if run_id is None:
            self._run_id = self._metadata.new_run_id()
        else:
            self._run_id = run_id
            self._metadata.register_run_id(run_id)

        self._datastore.datastore_root = datastore_root
        if self._datastore.datastore_root is None:
            self._datastore.datastore_root = self._datastore.get_datastore_root_from_config(self._logger.info)
        self._metadata.add_sticky_tags(tags=tags)
        #paths = decompress_list(input_paths) if input_paths else []
        paths = input_paths if input_paths else []
        self._input_paths = paths

        self._step_name = step_name
        self._clone_steps = {} if clone_steps is None else clone_steps
        step = self._flow.get_step(self._step_name)
        for deco in step.decorators:
            deco.runtime_init(self._flow,self._graph,None,run_id)
        
        if step_name == "start":
            parameters.set_parameters(self._flow, kwargs)
            self.persist_parameters()
            self._input_paths = [self._params_task]
        
        
    def persist_parameters(self, task_id=None):
        task = self._new_task('_parameters', task_id=task_id)
        if not task.is_cloned:
            task.persist(self._flow)
        self._params_task = task.path
        self._is_cloned[task.path] = task.is_cloned
    
    def _new_task(self, step, input_paths=None, **kwargs):
        #if input_paths is None:
        #    may_clone = True
        #else:
        #    may_clone = all(self._is_cloned[path] for path in input_paths)

        may_clone = False
        if step in self._clone_steps:
            may_clone = False

        if step == '_parameters':
            decos = []
        else:
            decos = getattr(self._flow, step).decorators

        return MetaflowTaskWrapper(self._datastore,
                    self._flow,
                    step,
                    self._run_id,
                    self._metadata,
                    self._environment,
                    self._entrypoint,
                    # self._logger,
                    # None,
                    input_paths=input_paths,
                    split_index=self._split_index,
                    may_clone=may_clone,
                    clone_run_id=self._clone_run_id,
                    origin_ds_set=self._origin_ds_set,
                    join_type=self._join_type,
                    logger=self._logger,
                    **kwargs)

    def execute(self):
        task = self._new_task(self._step_name, self._input_paths)
        task.run()
        triggers_list = self.on_complete(task)
        print("Step {} is spawning {} steps/tasks ".format(self._step_name, [s['step_name']+"/"+str(s["split_index"])+" --- "+str(s['input_paths']) for s in triggers_list]))
        return triggers_list

    def on_complete(self, task):

        trans = task.results.get('_transition')
        if trans:
            next_steps = trans[0]
            foreach = trans[1]
            condition = trans[2]
        else:
            next_steps = []
            foreach = None
        expected = self._graph[task.step].out_funcs

        if trans and condition:
            condition_val = getattr(self._flow, condition)
            if condition_val:
                next_steps = [next_steps[0]]
                expected = [expected[0]]
            else:
                next_steps = [next_steps[1]]
                expected = [expected[1]]
        # try:
        #     if next_steps[0] == 'merge_cup_disc_result':
        #         import pdb; pdb.set_trace()
        # except:
        # if expected == ["merge_AMD_Glaucoma"]:
            # import pdb; pdb.set_trace()
        if next_steps != expected:
                msg = 'Based on static analysis of the code, step *{step}* '\
                      'was expected to transition to step(s) *{expected}*. '\
                      'However, when the code was executed, self.next() was '\
                      'called with *{actual}*. Make sure there is only one '\
                      'unconditional self.next() call in the end of your '\
                      'step. '
                raise MetaflowInternalError(msg.format(step=task.step,
                                                       expected=', '.join(
                                                           expected),
                                                       actual=', '.join(next_steps)))
        triggers_list = []
        if any(self._graph[f].type == 'join' for f in next_steps):
                # Next step is a join
                # Trigger a join task with params (task, next_steps)
                res = self._trigger_task_join(task, next_steps)
                if res:
                    triggers_list.append(res)
        elif foreach:
                # Next step is a foreach child
                triggers_list += self._trigger_task_foreach(task, next_steps)
        else:
            # Next steps are normal linear steps
            for step in next_steps:
                # trigger normal task
                triggers_list.append(self._trigger_task(step, task, input_paths= [task.path]))

        return triggers_list

    def _trigger_task_join(self, task, next_steps):
        # if the next step is a join, we need to check that
        # all input tasks for the join have finished before queuing it.

        # CHECK: this condition should be enforced by the linter but
        # let's assert that the assumption holds
        if len(next_steps) > 1:
            msg = 'Step *{step}* transitions to a join and another '\
                  'step. The join must be the only transition.'
            raise MetaflowInternalError(task, msg.format(step=task.step))
        else:
            next_step = next_steps[0]

        # if next_step == "merge_cup_disc_result":
        #     import pdb; pdb.set_trace()

        # matching_split is the split-parent of the finished task
        matching_split = self._graph[self._graph[next_step].split_parents[-1]]
        step_name, foreach_stack = task.finished_id

        # TODO: Make this work
        # Change the logic to get it working

        if matching_split.type == 'foreach':
            # next step is a foreach join

            # def siblings(foreach_stack):
            #     top = foreach_stack[-1]
            #     bottom = list(foreach_stack[:-1])
            #     for index in range(top.num_splits):
            #         yield tuple(bottom + [top._replace(index=index)])

            # required tasks are all split-siblings of the finished task
            #required_tasks = [s[0] for s in siblings(foreach_stack)]
            #print("REQUIRED TASKS ... ", required_tasks)
            #num_splits = required_tasks[0].num_splits
            num_splits = foreach_stack[-1].num_splits
            #required_step = required_tasks[0].step
            required_step = step_name
            res = self._datastore.completed_step(
                    self._flow.name, 
                    run_id=self._run_id,
                    step=required_step)
            done = [item for item in res if item["done"] == True]
            if len(done) == num_splits:
                input_paths = ['%s/%s/%s' %(self._run_id, required_step, s["task_id"]) for s in done]
                join_type = 'foreach'
                return self._trigger_task(next_step, task,
                                join_type = join_type, input_paths=input_paths)
        elif matching_split.type == 'split-and':
            # next step is a split-and
            # required tasks are all branches joined by the next step
            #required_tasks = [self._finished.get((step, foreach_stack))
            #                 for step in self._graph[next_step].in_funcs]
            required_steps = [step for step in self._graph[next_step].in_funcs]
            #print("REQUIRED .... " ,required_steps)
            required_tasks = []
            input_paths = []
            for step in required_steps:
                res = self._datastore.completed_step(
                    self._flow.name, 
                    run_id=self._run_id,
                    step=step)
                # Check if any attempt is successfully completed
                completed_task = [item for item in res if item["done"] == True]

                if not completed_task:
                    return []

                completed_task = completed_task[0]
        
                required_tasks.append(completed_task)
                input_paths.append('%s/%s/%s' % (self._run_id, step, completed_task.get("task_id")))
            required_tasks = [res["done"] for res in required_tasks]
            join_type = 'linear'

            if all(required_tasks):
                # all tasks to be joined are ready. Schedule the next join step.
                return self._trigger_task(next_step, task,
                                join_type = join_type, input_paths=input_paths)
            #self._queue_push(next_step,
            #                 {'input_paths': required_tasks,
            #                  'join_type': join_type})
        elif matching_split.type == "split-or":
            join_type = 'split-or'
            return self._trigger_task(next_step, task, input_paths= [task.path], join_type=join_type)


    def _trigger_task_foreach(self, task, next_steps):

        # CHECK: this condition should be enforced by the linter but
        # let's assert that the assumption holds
        if len(next_steps) > 1:
            msg = 'Step *{step}* makes a foreach split but it defines '\
                  'multiple transitions. Specify only one transition '\
                  'for foreach.'
            raise MetaflowInternalError(msg.format(step=task.step))
        else:
            next_step = next_steps[0]

        num_splits = task.results['_foreach_num_splits']
        # if num_splits > self._max_num_splits:
        #     msg = 'Foreach in step *{step}* yielded {num} child steps '\
        #           'which is more than the current maximum of {max} '\
        #           'children. You can raise the maximum with the '\
        #           '--max-num-splits option. '
        #     raise TaskFailed(task, msg.format(step=task.step,
        #                                       num=num_splits,
        #                                       max=self._max_num_splits))

        # schedule all splits
        trigger_tasks_list = []
        for i in range(num_splits):

            tf = self._trigger_task(next_step, task,
                             split_index = i,
                              input_paths = [task.path])
            trigger_tasks_list.append(tf)
        return trigger_tasks_list

    def _trigger_task(self, step,task,input_paths=None, split_index=None, join_type=None):
        import os
        from .util import compress_list, get_username
        env = dict(os.environ)
        if not input_paths:
            input_paths = [task.path]
        params = {
            "flow": task.flow_name,
            "metadata" : task.metadata_type,
            "environment": task.environment_type,
            "datastore": task.datastore_type,
            "datastore_root": task.datastore_sysroot,
            "event_logger": None,
            "monitor": None,
            "step_name": step,
            "tags" : [],
            "run_id" : task.run_id,
            # task_id=None,
            #"input_paths": compress_list(task.path),
            "input_paths": input_paths,
            "split_index": split_index,
            "user_namespace": get_username() or '',
            "retry_count": 0,
            "max_user_code_retries": 100,
            "clone_only": None,
            "clone_run_id": None,
            "join_type": join_type 
        }

        return params
        #task = self._new_task(step, **task_kwargs)
        #Trigger task




class MetaflowTaskWrapper(object):
    clone_pathspec_mapping = {}

    def __init__(self,
                 datastore,
                 flow,
                 step,
                 run_id,
                 metadata,
                 environment,
                 entrypoint,
                #  event_logger,
                #  monitor,
                 input_paths=None,
                 split_index=None,
                 clone_run_id=None,
                 origin_ds_set=None,
                 may_clone=False,
                 join_type=None,
                 logger=None,
                 task_id=None,
                 decos=[]):
        if task_id is None:
            task_id = str(metadata.new_task_id(run_id, step))
        else:
            # task_id is preset only by persist_parameters()
            metadata.register_task_id(run_id, step, task_id)

        self.flow = flow
        self.step = step
        self.flow_name = flow.name
        self.run_id = run_id
        self.task_id = task_id
        self.input_paths = input_paths
        self.split_index = split_index
        self.decos = decos
        self.entrypoint = entrypoint
        self.environment = environment
        self.environment_type = self.environment.TYPE
        self.clone_run_id = clone_run_id
        self.clone_origin = None
        self.origin_ds_set = origin_ds_set
        self.metadata = metadata
        # self.event_logger = event_logger
        # self.monitor = monitor

        self._logger = logger
        self._path = '%s/%s/%s' % (self.run_id, self.step, self.task_id)

        self.retries = 0
        self.user_code_retries = 0
        self.error_retries = 0

        self.tags = metadata.sticky_tags
        #self.event_logger_type = self.event_logger.logger_type
        # self.event_logger_type = None
        #self.monitor_type = monitor.monitor_type
        # self.monitor_type = None

        self.metadata_type = metadata.TYPE
        self.datastore_type = datastore.TYPE
        self._datastore = datastore
        self.datastore_sysroot = datastore.datastore_root
        self._results_ds = None

        if clone_run_id and may_clone:
            self._is_cloned = self._attempt_clone(clone_run_id, join_type)
        else:
            self._is_cloned = False

        # Open the output datastore only if the task is not being cloned.
        if not self._is_cloned:
            self.new_attempt()

            for deco in decos:
                deco.runtime_task_created(self._ds,
                                          task_id,
                                          split_index,
                                          input_paths,
                                          self._is_cloned)

                # determine the number of retries of this task
                user_code_retries, error_retries = deco.step_task_retry_count()
                self.user_code_retries = max(self.user_code_retries,
                                             user_code_retries)
                self.error_retries = max(self.error_retries, error_retries)

    def new_attempt(self):
        self._ds = self._datastore(self.flow_name,
                                   run_id=self.run_id,
                                   step_name=self.step,
                                   task_id=self.task_id,
                                   mode='w',
                                   metadata=self.metadata,
                                   attempt=self.retries)
                                #    event_logger=self.event_logger,
                                #    monitor=self.monitor)


    def log(self, msg, system_msg=False, pid=None):
        if pid:
            prefix = '[%s (pid %s)] ' % (self._path, pid)
        else:
            prefix = '[%s] ' % self._path

        self._logger(msg, head=prefix, system_msg=system_msg)
        sys.stdout.flush()

    def _find_origin_task(self, clone_run_id, join_type):
        if self.step == '_parameters':
            pathspec = '%s/_parameters[]' % clone_run_id
            origin = self.origin_ds_set.get_with_pathspec_index(pathspec)

            if origin is None:
                # This is just for usability: We could rerun the whole flow
                # if an unknown clone_run_id is provided but probably this is
                # not what the user intended, so raise a warning
                raise MetaflowException("Resume could not find run id *%s*" %
                                        clone_run_id)
            else:
                return origin
        else:
            # all inputs must have the same foreach stack, so we can safely
            # pick the first one
            parent_pathspec = self.input_paths[0]
            origin_parent_pathspec = \
                self.clone_pathspec_mapping[parent_pathspec]
            parent = self.origin_ds_set.get_with_pathspec(origin_parent_pathspec)
            #parent = self.origin_ds_set.get_with_pathspec(parent_pathspec)
            # Parent should be non-None since only clone the child if the parent
            # was successfully cloned.
            foreach_stack = parent['_foreach_stack']
            if join_type == 'foreach':
                # foreach-join pops the topmost index
                index = ','.join(str(s.index) for s in foreach_stack[:-1])
            elif self.split_index:
                # foreach-split pushes a new index
                index = ','.join([str(s.index) for s in foreach_stack] +
                                 [str(self.split_index)])
            else:
                # all other transitions keep the parent's foreach stack intact
                index = ','.join(str(s.index) for s in foreach_stack)
            pathspec = '%s/%s[%s]' % (clone_run_id, self.step, index)
            return self.origin_ds_set.get_with_pathspec_index(pathspec)

    def _attempt_clone(self, clone_run_id, join_type):
        origin = self._find_origin_task(clone_run_id, join_type)

        if origin and origin['_task_ok']:
            # Store the mapping from current_pathspec -> origin_pathspec which
            # will be useful for looking up origin_ds_set in find_origin_task.
            self.clone_pathspec_mapping[self._path] = origin.pathspec
            if self.step == '_parameters':
                # Clone in place without relying on run_queue.
                self.new_attempt()
                self._ds.clone(origin)
                self._ds.done()
            else:
                self.log("Cloning results of a previously run task %s"
                         % origin.pathspec, system_msg=True)
                # Store the origin pathspec in clone_origin so this can be run
                # as a task by the runtime.
                self.clone_origin = origin.pathspec
                # Save a call to creating the results_ds since its same as origin.
                self._results_ds = origin
            return True
        else:
            return False

    @property
    def path(self):
        return self._path

    @property
    def results(self):
        if self._results_ds:
            return self._results_ds
        else:
            self._results_ds = self._datastore(self.flow_name,
                                               run_id=self.run_id,
                                               step_name=self.step,
                                               task_id=self.task_id,
                                               mode='r',
                                               metadata=self.metadata,
                                               event_logger=self._logger)
                                            #    monitor=self.monitor)
            return self._results_ds

    @property
    def finished_id(self):
        # note: id is not available before the task has finished
        return (self.step, tuple(self.results['_foreach_stack']))

    @property
    def is_cloned(self):
        return self._is_cloned

    def persist(self, flow):
        # this is used to persist parameters before the start step
        flow._task_ok = flow._success = True
        flow._foreach_stack = []
        self._ds.persist(flow)
        self._ds.done()

    def save_logs(self, logtype, logs):
        location = self._ds.save_log(logtype, logs)
        datum = [MetaDatum(field='log_location_%s' % logtype,
                           value=json.dumps({
                               'ds_type': self._ds.TYPE,
                               'location': location,
                               'attempt': self.retries}),
                           type='log_path')]
        self.metadata.register_metadata(self.run_id,
                                        self.step,
                                        self.task_id,
                                        datum)
        return location

    def save_metadata(self, name, metadata):
        self._ds.save_metadata(name, metadata)

    def __str__(self):
        return ' '.join(self._args)
    
    def run(self):
        metaflow_task = MetaflowTask(
            self.flow,
            self._datastore,
            self.metadata,
            self.environment,
            self._logger
        )
        metaflow_task.run_step(
            self.step,
            self.run_id,
            self.task_id,
            self.clone_run_id,
            self.input_paths,
            self.split_index,
            self.retries,
            self.user_code_retries
        )


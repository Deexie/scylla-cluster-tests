from functools import partial

from sdcm.tester import ClusterTester
from sdcm.utils.common import ParallelObject
from sdcm.utils.decorators import measure_time

class MixedShardRepair(ClusterTester):

    def setUp(self):
        super().setUp()
        self.populate_data_parallel(size_in_gb=1024, blocking=True)

    @measure_time
    def _run_repair(self, node):
        self.log.info('Running nodetool repair on {}'.format(node.name))
        node.run_nodetool(sub_cmd='repair')

    @measure_time
    def _run_repairs(self):
        triggers = [partial(node.run_nodetool, sub_cmd="repair", args=f"keyspace1 standard1", ) for node
                    in self.db_cluster.nodes]
        ParallelObject(objects=triggers, timeout=1200 * 60).call_objects()

    def test_mixed_shard_repair(self):
        node = self.db_cluster.nodes[0]
        repair_time = self._run_repair(node=node)[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair time on node: {} is: {}'.format(node.name, repair_time))

    def test_mixed_shard_repair_on_all_nodes(self):
        repair_time = self._run_repairs()[0]  # pylint: disable=unsubscriptable-object
        self.log.info('Repair time is: {}'.format(repair_time))

    def test_mixed_shard_bootstrap(self):
        self.log.info('Bootstrapping a new node...')
        new_node = self.db_cluster.add_nodes(count=1)[0]
        self.monitors.reconfigure_scylla_monitoring()
        self.log.info('Waiting for new node to finish initializing...')
        self.db_cluster.wait_for_init(node_list=[new_node])
        self.monitors.reconfigure_scylla_monitoring()

import json
import logging

import queue

from obnl.core.client import ClientNode

from ict.connection.node import Node

from ict.protobuf.backend.simulation_pb2 import *
from ict.protobuf.backend.db_pb2 import *
from ict.protobuf.backend.test_pb2 import *
from ict.protobuf.default_pb2 import MetaMessage


class ClientTestNode(ClientNode):

    def __init__(self, host, vhost, username, password, config_file,
                 api, data, name = None,
                 input_attributes=None, output_attributes=None, is_first=False):
        super().__init__(host, vhost, username, password, config_file,
                         input_attributes, output_attributes, is_first)
        self._node_impl.activate_console_logging(logging.DEBUG)
        if name:
            self._node_impl._name = name
        self._api = api
        self._data = data
        self._i = 0

    def step(self, current_time, time_step):
        print('----- '+self.name+' -----')
        print(self.name, time_step)
        print(self.name, current_time)
        print(self.name, self.input_values)

        for o in self.output_attributes:
            rv = self._data[0 % len(self._data)]
            print(self.name, o, ':', rv)
            self.update_attribute(o, rv)
        print('=============')


class Wrapper(Node):
    def __init__(self, host, vhost, username, password, config_file,
                 input_attr, output_attr):
        super().__init__(host, vhost, username, password, config_file)

        self._input_attr = input_attr
        self._output_attr = output_attr

        self._queue = queue.Queue()

    def on_block(self, ch, method, props, body):
        Node.LOGGER.info(self._name + " receives a block message.")
        m = MetaMessage()
        m.ParseFromString(body)

        if m.details.Is(DataRequired.DESCRIPTOR):
            dr = DataRequired()
            m.details.Unpack(dr)

            fwd = MetaMessage()
            fwd.node_name = self._name

            fwd.details.Pack(dr)

            self.send('', 'db.data.init', fwd.SerializeToString(),
                      reply_to='coside.cosim.simu.' + SimulationBlock.Name(dr.block) + '.' + self.name)

        elif m.details.Is(TestNodeInfo.DESCRIPTOR):
            tni = TestNodeInfo()
            Node.LOGGER.debug("receive " + str(type(tni)))
            m.details.Unpack(tni)

            pn_node = ClientTestNode(host=self.host,
                                     vhost='obnl_vhost',
                                     username='obnl',
                                     password='obnl',
                                     config_file="wrapper/default.json",
                                     api=self,
                                     data=tni.values,
                                     name=self.name,
                                     input_attributes=self._input_attr,
                                     output_attributes=self._output_attr,
                                     is_first=True)
            self.send('', 'wrapper.local.'+self.name, "next")
            pn_node.start()

        self._channel.basic_ack(delivery_tag=method.delivery_tag)

    def add_message(self, message):
        self._queue.put(message)

    def on_local(self, ch, method, props, body):

        try:
            message = self._queue.get(timeout=0.1)
            self.send('', 'db.data.store', message.SerializeToString())
        except queue.Empty:
            pass

        self._channel.basic_ack(delivery_tag=method.delivery_tag)
        self.send('', 'wrapper.local.'+self.name, "next")


if __name__ == "__main__":
    Node.activate_console_logging(logging.DEBUG)

    print(sys.argv[1])
    print(sys.argv[2])

    input_attr = json.loads(sys.argv[1])
    output_attr = json.loads(sys.argv[2])

    w = Wrapper("172.17.0.1", "backend_vhost", "tool", "tool", sys.argv[3],
                input_attr, output_attr)
    w.start()

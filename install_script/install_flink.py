# -*- coding: utf-8 -*-
from commons import *


def install_flink():
    flink_cong_template ="""
rest.address, rest.port=
jobmanager.rpc.address=
jobmanager.memory.process.size=
taskmanager.memory.process.size=
taskmanager.numberOfTaskSlots=
parallelism.default=
io.tmp.dirs=

"""

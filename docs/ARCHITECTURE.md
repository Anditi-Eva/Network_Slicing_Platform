# Architecture

Client → Controller/Core → Worker Node 1 / Worker Node 2

Controller: slice management, scheduling, resource-aware worker selection, coordination.

Workers: process execution, resource monitoring, slice allocation and logical-clock event handling.

Communication: REST/HTTP.

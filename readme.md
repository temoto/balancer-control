What
====

Control balancer (nginx) from other machines (Lambda) via message queue (SQS).

Status
======

Archived, code is preserved as fossil.

After some intensive development, I discovered Consul https://www.consul.io/ which allows same results with `proxy_pass backend.service.consul` or consul-template or fabio. I'm very happy balancer-control project wasn't finished as Consul is much better alternative from every point of view.

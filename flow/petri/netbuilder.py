import pygraphviz
import safenet as sn

def _make_transition_action(conn, transition):
    if transition.action_class is None:
        return None

    return transition.action_class.create(
            conn,
            name=transition.name,
            args=transition.action_args,
            place_refs=transition.place_refs)

def _compile_net(connection, net):
    transition_actions = [_make_transition_action(connection, x)
                          for x in net.transitions]
    return sn.SafeNet.create(
            connection=connection,
            name=net.name,
            place_names=net.places,
            trans_actions=transition_actions,
            place_arcs_out=net.place_arcs_out,
            trans_arcs_out=net.trans_arcs_out)


class Transition(object):
    def __init__(self, name, action_class=None, action_args=None, place_refs=None):
        self.name = name
        self.action_class = action_class
        self.action_args = action_args
        self.place_refs = place_refs


class Net(object):
    def __init__(self, name):
        self.name = name
        self.places = []
        self.transitions = []
        self.place_arcs_out = {}
        self.trans_arcs_out = {}

    def add_place(self, name):
        self.places.append(name)
        return len(self.places) - 1

    def add_transition(self, transition):
        self.transitions.append(transition)
        return len(self.transitions) - 1

    def add_place_arc_out(self, src, dst):
        self.place_arcs_out.setdefault(src, set()).add(dst)

    def add_trans_arc_out(self, src, dst):
        self.trans_arcs_out.setdefault(src, set()).add(dst)

    def graph(self):
        graph = pygraphviz.AGraph(directed=True)

        for i, p in enumerate(self.places):
            graph.add_node("p%d" % i, label=p)

        for i, t in enumerate(self.transitions):
            graph.add_node("t%d" % i, label=t.name, shape="box",
                    style="filled", fillcolor="black", fontcolor="white")

        for src, dst_set in self.place_arcs_out.iteritems():
            for dst in dst_set:
                pid = "p%d" % src
                tid = "t%d" % dst
                graph.add_edge(pid, tid)

        for src, dst_set in self.trans_arcs_out.iteritems():
            for dst in dst_set:
                tid = "t%d" % src
                pid = "p%d" % dst
                graph.add_edge(tid, pid)

        return graph


if __name__ == "__main__":
    from flow.orchestrator.client import OrchestratorClient
    import flow.brokers.amqp
    import flow.orchestrator.redisom as rom
    import flow.petri.safenet as sn
    import json
    import os
    import pika
    import redis
    import sys


    net = Net("hi")
    start = net.add_place("start")
    msg_ok = net.add_place("msg_ok")
    msg_fail = net.add_place("msg_fail")


    t_start = net.add_transition(
            Transition(
                name="t_start",
                action_class=sn.ShellCommandAction,
                action_args=["df", "/"],
                place_refs=[msg_ok, msg_fail]
            )
    )

    running = net.add_place("running")

    t_ok = net.add_transition(Transition(name="t_ok"))

    t_fail = net.add_transition(Transition(name="t_fail"))

    ok = net.add_place("ok")
    fail = net.add_place("fail")

    net.add_place_arc_out(start, t_start)
    net.add_trans_arc_out(t_start, running)
    net.add_place_arc_out(running, t_ok)
    net.add_place_arc_out(running, t_fail)
    net.add_place_arc_out(msg_ok, t_ok)
    net.add_place_arc_out(msg_fail, t_fail)
    net.add_trans_arc_out(t_ok, ok)
    net.add_trans_arc_out(t_fail, fail)

    conn = redis.Redis()
    cnet = _compile_net(conn, net)
    net_key = cnet.key
    print "Compiled the net, key is", net_key

    token = sn.Token.create(conn)

    routing_key = "petri.place.set_token"
    body = json.dumps({
        "net_key": net_key,
        "place_idx": 0,
        "token_key": token.key,
        "message_class": "SetTokenMessage",
    })

    print "Publishing message", body

    amqp_url = os.environ['AMQP_URL']
    conn = pika.BlockingConnection(pika.URLParameters(amqp_url))
    qchannel = conn.channel()
    qchannel.basic_publish(
        exchange="workflow",
        routing_key=routing_key,
        body=body,
        properties=pika.BasicProperties(
            delivery_mode=2,
        )
    )

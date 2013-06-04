from collections import namedtuple

import flow.redisom as rom


Color = int


ColorGroup = namedtuple("ColorGroup", ["idx", "parent_color",
        "parent_color_group", "begin", "end"])

ColorGroup.size = property(lambda self: self.end - self.begin)
ColorGroup.colors = property(lambda self: range(self.begin, self.end))
ColorGroup.color_iter = property(lambda self: xrange(self.begin, self.end))


def color_group_enc(value):
    return rom.json_enc(value._asdict())


def color_group_dec(value):
    return ColorGroup(**rom.json_dec(value))


ColorDescriptor = namedtuple("ColorDescriptor", ["color", "group"])

import flow.redisom as rom
from collections import namedtuple

ColorGroup = namedtuple("ColorGroup", ["idx", "parent_color",
        "parent_color_group", "begin", "end"])

class Net(rom.Object):
    next_color = rom.Property(rom.Int)
    next_color_group = rom.Property(rom.Int)
    color_groups = rom.Property(rom.Hash, value_encoder=rom.json_enc,
            value_decoder=rom.json_dec)

    def color_group(self, idx):
        return ColorGroup(**self.color_groups[idx])

    def add_color_group(self, parent_color, parent_color_group, size):
        group_id = self.next_color_group.incr(1)
        end = self.next_color.incr(size)
        start = end - size

        cg = ColorGroup(idx=group_id, parent_color=parent_color,
                parent_color_group=parent_color_group, start=start, end=end)

        self.color_groups[group_id] = cg._asdict()
        return cg

"""
Read binary change data and compute binary change statistics.
Author: Daan Kooij
Last modified: October 11th, 2021
"""

from collections import defaultdict
from datetime import datetime

import plotly.express as px
import plotly.graph_objects as go


INPUT_FILE_PATH = "input/change-data-combined.csv"
FIRST_WEEK = 24
LAST_WEEK = 35


def compute_change_dict():
    change_dict = defaultdict(lambda: defaultdict(int))

    with open(INPUT_FILE_PATH) as input_file:
        for line in input_file:
            [page_id, date_str, change_str] = line.strip().split(",")
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            week = date_obj.isocalendar()[1]
            change_dict[page_id][week] += int(change_str)

    return change_dict


def compute_changes_per_week(change_dict):
    change_matrix = []
    for week_number in range(FIRST_WEEK, LAST_WEEK + 1):
        change_list = [0 for _ in range(8)]
        for changes_per_week_dict in change_dict.values():
            change_list[changes_per_week_dict[week_number]] += 1
        change_matrix.append(change_list)
    return change_matrix


def compute_change_cube(change_dict):
    change_cube = []  # change_cube[week_number][prev_num_changes][cur_num_changes] = occurrences
    for week_number in range(FIRST_WEEK + 1, LAST_WEEK + 1):  # Only after first week

        # change_matrix[prev_num_changes][cur_num_changes] = occurrences
        change_matrix = [[0 for _ in range(8)] for _ in range(8)]
        for page_id, changes_per_week_dict in change_dict.items():
            previous_week_changes = changes_per_week_dict[week_number - 1]
            current_week_changes = changes_per_week_dict[week_number]
            change_matrix[previous_week_changes][current_week_changes] += 1

        change_cube.append(change_matrix)

    return change_cube


def draw_alluvial_plot(change_cube):
    sources, targets, values, link_colors = [], [], [], []
    node_labels, node_colors = [], []

    def get_color(color_index, opacity=1.0):
        color_code = px.colors.qualitative.Plotly[color_index]
        red, green, blue = color_code[1:3], color_code[3:5], color_code[5:7]
        red, green, blue = str(int(red, 16)), str(int(green, 16)), str(int(blue, 16))
        return "rgba(" + red + "," + green + "," + blue + "," + str(opacity) + ")"

    for i in range(8):
        node_labels.append(str(i))
        node_colors.append(get_color(i))

    for stage, change_matrix in zip(range(len(change_cube)), change_cube):
        for i in range(8):
            for j in range(8):
                sources.append(stage * 8 + i)
                targets.append((stage + 1) * 8 + j)
                values.append(change_matrix[i][j])
                link_colors.append(get_color(i, opacity=0.5))
            node_labels.append(str(i))
            node_colors.append(get_color(i))

    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=15,
            thickness=20,
            line=dict(color="black", width=0.5),
            color=node_colors,
            label=node_labels
        ),
        link=dict(
            source=sources,  # indices correspond to node_labels, eg A1, A2, A1, B1, ...
            target=targets,
            value=values,
            color=link_colors
        ))])

    fig.update_layout(title_text="Alluvial Plot depicting the number of days "
                                 "that pages in the Dutch Web change per week, in weeks 24-35 of 2021.")
    fig.show()


# cd = compute_change_dict()
# cm = compute_changes_per_week(cd)
# cc = compute_change_cube(cd)
#
# print("cm =", cm)
# print("cc =", cc)

# cm = [[74892, 11671, 12029, 6819, 6330, 6651, 4605, 21046], [74478, 11779, 12044, 6880, 6414, 6739, 4583, 21126], [73898, 12073, 12024, 7040, 6407, 6472, 4900, 21229], [75584, 11597, 11665, 6537, 6295, 6497, 4526, 21342], [76366, 11579, 11433, 6702, 6166, 6163, 4484, 21150], [78157, 10790, 11333, 6154, 5754, 6100, 4502, 21253], [78081, 11194, 11348, 6197, 5901, 5984, 4235, 21103], [79749, 10093, 10961, 6115, 5458, 5408, 4466, 21793], [76712, 6896, 13719, 9137, 7778, 6234, 19044, 4523], [85277, 16676, 24844, 3071, 2940, 2421, 4231, 4583], [78422, 10619, 10709, 6076, 5922, 5854, 4495, 21946], [78854, 12780, 10450, 7368, 6594, 16873, 2211, 8913]]
cc = [[[63501, 5479, 4336, 858, 517, 139, 49, 13], [5076, 2871, 1850, 982, 558, 219, 84, 31], [4318, 1787, 2653, 1336, 1121, 520, 202, 92], [855, 839, 1348, 1275, 1083, 822, 379, 218], [536, 444, 1031, 1094, 1264, 983, 599, 379], [144, 235, 491, 757, 902, 1450, 858, 1814], [37, 75, 213, 362, 561, 897, 1246, 1214], [11, 49, 122, 216, 408, 1709, 1166, 17365]], [[62517, 5813, 4430, 981, 547, 127, 57, 6], [5319, 2868, 1742, 968, 501, 262, 99, 20], [4382, 1688, 2685, 1415, 1106, 488, 208, 72], [965, 920, 1320, 1362, 1061, 700, 331, 221], [543, 463, 1075, 1107, 1208, 978, 678, 362], [113, 213, 490, 685, 1021, 1454, 988, 1775], [53, 78, 215, 311, 538, 883, 1262, 1243], [6, 30, 67, 211, 425, 1580, 1277, 17530]], [[63526, 4980, 3870, 734, 569, 172, 37, 10], [5946, 2876, 1697, 840, 428, 181, 70, 35], [4378, 1832, 2758, 1302, 956, 537, 187, 74], [925, 1047, 1415, 1340, 1048, 706, 317, 242], [555, 528, 1076, 996, 1285, 952, 572, 443], [142, 206, 490, 739, 963, 1381, 844, 1707], [99, 91, 229, 346, 625, 857, 1272, 1381], [13, 37, 130, 240, 421, 1711, 1227, 17450]], [[65186, 4999, 3873, 787, 513, 139, 60, 27], [5143, 3082, 1679, 905, 467, 203, 69, 49], [4353, 1727, 2668, 1219, 1015, 412, 170, 101], [949, 952, 1233, 1200, 999, 699, 305, 200], [519, 496, 1111, 1135, 1196, 960, 545, 333], [165, 219, 582, 863, 957, 1349, 785, 1577], [44, 68, 207, 361, 599, 846, 1303, 1098], [7, 36, 80, 232, 420, 1555, 1247, 17765]], [[66186, 4523, 4215, 736, 523, 117, 60, 6], [5907, 2670, 1537, 721, 410, 224, 72, 38], [4350, 1615, 2505, 1222, 945, 461, 247, 88], [898, 1030, 1309, 1276, 918, 733, 339, 199], [566, 587, 1009, 999, 1193, 883, 598, 331], [194, 253, 482, 700, 887, 1351, 782, 1514], [37, 82, 207, 317, 557, 957, 1190, 1137], [19, 30, 69, 183, 321, 1374, 1214, 17940]], [[67039, 5297, 4233, 784, 575, 146, 63, 20], [5040, 2451, 1621, 859, 497, 233, 60, 29], [4273, 1689, 2531, 1215, 974, 419, 160, 72], [814, 915, 1232, 1134, 965, 586, 326, 182], [642, 459, 948, 937, 1091, 887, 490, 300], [214, 267, 489, 719, 899, 1270, 817, 1425], [47, 87, 227, 325, 594, 910, 1209, 1103], [12, 29, 67, 224, 306, 1533, 1110, 17972]], [[68302, 4521, 3725, 725, 536, 145, 119, 8], [5337, 2590, 1558, 937, 441, 216, 86, 29], [4553, 1408, 2617, 1111, 914, 421, 206, 118], [871, 822, 1239, 1160, 919, 667, 318, 201], [527, 423, 1088, 1017, 1090, 856, 566, 334], [109, 219, 463, 701, 800, 1206, 839, 1647], [43, 89, 198, 299, 497, 771, 1112, 1226], [7, 21, 73, 165, 261, 1126, 1220, 18230]], [[67334, 2911, 6308, 2237, 744, 174, 39, 2], [4335, 1638, 1803, 1407, 596, 248, 63, 3], [3693, 1141, 2615, 1597, 1203, 535, 158, 19], [662, 571, 1125, 1449, 1168, 687, 410, 43], [489, 338, 925, 1062, 1168, 873, 523, 80], [123, 179, 441, 725, 1093, 1263, 1362, 222], [59, 70, 229, 404, 708, 1248, 1453, 295], [17, 48, 273, 256, 1098, 1206, 15036, 3859]], [[70438, 3687, 1893, 352, 239, 68, 33, 2], [3828, 1568, 820, 354, 188, 95, 33, 10], [5831, 4291, 2118, 635, 490, 241, 89, 24], [3032, 2632, 1749, 656, 539, 333, 152, 44], [1437, 1904, 2368, 547, 606, 435, 388, 93], [459, 1324, 2425, 327, 442, 518, 454, 285], [249, 1261, 13445, 162, 373, 470, 2777, 307], [3, 9, 26, 38, 63, 261, 305, 3818]], [[70645, 6284, 4937, 1462, 1104, 446, 232, 167], [5182, 2548, 2592, 1811, 1530, 1216, 760, 1037], [1980, 1081, 1811, 1289, 1569, 2199, 1795, 13120], [326, 375, 606, 600, 507, 372, 182, 103], [214, 169, 442, 484, 559, 469, 309, 294], [55, 120, 209, 265, 356, 482, 447, 487], [17, 32, 79, 121, 217, 430, 484, 2851], [3, 10, 33, 44, 80, 240, 286, 3887]], [[68569, 5520, 3090, 748, 352, 101, 33, 9], [4843, 2852, 1493, 823, 375, 169, 45, 19], [3848, 2114, 2214, 1168, 843, 342, 130, 50], [799, 1080, 1267, 1257, 845, 556, 176, 96], [589, 671, 1230, 1149, 1050, 799, 282, 152], [137, 332, 674, 1000, 1061, 1651, 415, 584], [57, 140, 343, 577, 1034, 1191, 578, 575], [12, 71, 139, 646, 1034, 12064, 552, 7428]]]

draw_alluvial_plot(cc)

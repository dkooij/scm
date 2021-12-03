"""
Read binary change data and compute binary change statistics.
Author: Daan Kooij
Last modified: December 3rd, 2021
"""

from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
import plotly.express as px
import plotly.graph_objects as go


INPUT_FILE_PATH = "inputmisc/change-data-combined.csv"
FIRST_WEEK = 24
LAST_WEEK = 35
PLOT_LINE_WIDTH = 3.2


def compute_change_dicts():
    # change_dict: {change_type -> {page_id -> {week -> number_of_changes}}}
    change_dicts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    with open(INPUT_FILE_PATH) as input_file:
        for line in input_file:
            [page_id, date_str, text_change_str, ilink_change_str, elink_change_str] = line.strip().split(",")
            date_obj = datetime.strptime(date_str, "%Y%m%d")
            week = date_obj.isocalendar()[1]
            change_dicts["Page text"][page_id][week] += int(text_change_str)
            change_dicts["Internal outlinks"][page_id][week] += int(ilink_change_str)
            change_dicts["External outlinks"][page_id][week] += int(elink_change_str)

    return change_dicts


def compute_changes_per_week(change_dicts):
    change_matrices = defaultdict(list)
    for change_type, change_dict in change_dicts.items():
        for week_number in range(FIRST_WEEK, LAST_WEEK + 1):
            change_list = [0 for _ in range(8)]
            for changes_per_week_dict in change_dict.values():
                change_list[changes_per_week_dict[week_number]] += 1
            change_matrices[change_type].append(change_list)
    return change_matrices


def compute_change_cube(change_dicts):
    # change_cubes: {change_type -> change_cube}
    # change_cubes[change_type][week_number][prev_num_changes][cur_num_changes] = occurrences
    change_cubes = defaultdict(list)  #
    for change_type, change_dict in change_dicts.items():
        for week_number in range(FIRST_WEEK + 1, LAST_WEEK + 1):  # Only after first week

            # change_matrix[prev_num_changes][cur_num_changes] = occurrences
            change_matrix = [[0 for _ in range(8)] for _ in range(8)]
            for page_id, changes_per_week_dict in change_dict.items():
                previous_week_changes = changes_per_week_dict[week_number - 1]
                current_week_changes = changes_per_week_dict[week_number]
                change_matrix[previous_week_changes][current_week_changes] += 1

            change_cubes[change_type].append(change_matrix)

    return change_cubes


def compute_page_changes_per_day():
    changes_per_day, start_date = defaultdict(list), None

    with open(INPUT_FILE_PATH) as input_file:
        for line in input_file:
            [_, date_str, text_change_str, ilink_change_str, elink_change_str] = line.strip().split(",")
            date_obj = datetime.strptime(date_str, "%Y%m%d")

            if start_date is None:
                start_date = date_obj
            day_number = (date_obj - start_date).days

            while day_number >= len(changes_per_day["Page text"]):
                changes_per_day["Page text"].append(0)
                changes_per_day["Internal outlinks"].append(0)
                changes_per_day["External outlinks"].append(0)
            changes_per_day["Page text"][day_number] += int(text_change_str)
            changes_per_day["Internal outlinks"][day_number] += int(ilink_change_str)
            changes_per_day["External outlinks"][day_number] += int(elink_change_str)

    return changes_per_day


def draw_alluvial_plot(change_cube):
    sources, targets, values, link_colors = [], [], [], []
    node_labels, node_colors = [], []

    def get_color(color_index, opacity=1.0):
        color_code_rgb = px.colors.qualitative.Dark2[color_index]
        [red, green, blue] = color_code_rgb.split("(")[1].split(")")[0].split(",")
        return "rgba(" + red + "," + green + "," + blue + "," + str(opacity) + ")"

    for i in range(len(change_cube[0])):
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

    fig = go.Figure(
        data=[go.Sankey(
            arrangement="snap",
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
            )
        )])

    fig.update_layout(title_text="Alluvial Plot depicting the number of days "
                                 "that pages in the Dutch Web change per week, in weeks 24-35 of 2021")
    fig.show()


def draw_page_changes_per_day(changes_per_day):
    day_numbers = list(range(2, len(changes_per_day) + 2))

    plt.figure()
    plt.plot(day_numbers, changes_per_day, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Number of page changes per day")
    plt.xlabel("Day number")
    plt.ylabel("Pages changed")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/changes-per-day.png", dpi=400)


def draw_different_change_behaviour_fractions(change_cube):
    week_numbers = list(range(FIRST_WEEK + 1, LAST_WEEK + 1))
    fractions = []  # Percentage of pages that exhibit different change behaviour in following week, per week
    for change_matrix in change_cube:
        week_same, total = 0, 0
        for i in range(len(change_cube[0])):
            week_same += change_matrix[i][i]
            total += sum(change_matrix[i])
        week_different = total - week_same
        fractions.append(week_different / total)

    plt.figure()
    plt.plot(week_numbers, fractions, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Percentage of pages that exhibit different change behaviour in following week, per week")
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-different-change-behaviour.png", dpi=400)


def draw_same_change_behaviour_fractions(change_cube):
    # Percentage of pages that continue to exhibit same change behaviour
    # in following week, per change behaviour per week
    week_numbers = list(range(FIRST_WEEK + 1, LAST_WEEK + 1))
    fractions_per_change_behaviour = [[] for _ in range(len(change_cube[0]))]
    for change_matrix in change_cube:
        for i in range(len(change_matrix)):
            week_cb_same = change_matrix[i][i]
            week_cb_total = sum(change_matrix[i])
            fractions_per_change_behaviour[i].append(week_cb_same / week_cb_total)

    plt.figure()
    for i, fractions_list in zip(range(len(fractions_per_change_behaviour)), fractions_per_change_behaviour):
        plt.plot(week_numbers, fractions_list, linewidth=2.5, color=plt.cm.Dark2(i), label=str(i))
    plt.title("Percentage of pages that continue to exhibit same change behaviour "
              "in following week, per change behaviour per week")
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-same-change-behaviour.png", dpi=400)


def draw_fraction_not_changed_weekly(change_matrix):
    # Fraction of pages not changed, per week
    week_numbers = list(range(FIRST_WEEK, LAST_WEEK + 1))
    fractions_not_changed = [week_changes[0] / sum(week_changes) for week_changes in change_matrix]

    plt.figure()
    plt.plot(week_numbers, fractions_not_changed, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Fraction of pages not changed, per week")
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-fraction-not-changed.png", dpi=400)


def draw_fraction_changed_every_day_weekly(change_matrix):
    # Fraction of pages changed every day, per week
    week_numbers = list(range(FIRST_WEEK, LAST_WEEK + 1))
    fraction_always_changed = [week_changes[-1] / sum(week_changes) for week_changes in change_matrix]

    plt.figure()
    plt.plot(week_numbers, fraction_always_changed, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Fraction of pages changed every day, per week")
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-fraction-always-changed.png", dpi=400)


# cd = compute_change_dicts()
# cm = compute_changes_per_week(cd)
# cc = compute_change_cube(cd)
# cpd = compute_page_changes_per_day()

cm = {"Page text": [[81657, 11257, 9856, 6468, 6033, 6558, 4420, 20110], [80941, 11417, 9949, 6527, 6121, 6743, 4421, 20240], [79878, 11775, 10306, 6666, 6096, 6546, 4771, 20321], [81592, 11170, 9982, 6231, 5998, 6545, 4465, 20376], [81958, 11251, 9808, 6435, 5985, 6209, 4373, 20340], [84266, 10472, 9373, 5851, 5510, 6179, 4318, 20390], [84354, 10734, 9432, 5796, 5604, 5965, 4157, 20317], [84872, 9720, 9633, 5907, 5269, 5541, 4380, 21037], [81382, 6609, 12503, 9175, 7742, 6149, 18356, 4443], [89542, 16149, 23782, 3007, 2857, 2387, 4154, 4481], [83101, 10470, 9476, 5913, 5808, 5955, 4425, 21211], [82534, 12423, 9980, 7305, 6660, 16515, 2192, 8750]], "Internal outlinks": [[96163, 10334, 7973, 4941, 4778, 4865, 3321, 13984], [95803, 10572, 7836, 5097, 4680, 4998, 3071, 14302], [95180, 10911, 8218, 4968, 4523, 4872, 3381, 14306], [96615, 10259, 7767, 4738, 4527, 4799, 3239, 14415], [96128, 10749, 8061, 4999, 4389, 4522, 3145, 14366], [98194, 9897, 7594, 4671, 4094, 4375, 3213, 14321], [98776, 9739, 7497, 4511, 4197, 4268, 3072, 14299], [99583, 9051, 7569, 4445, 3783, 4129, 2925, 14874], [96178, 6060, 10253, 7682, 5744, 4447, 13520, 2475], [103585, 13554, 18221, 2218, 1978, 1590, 2670, 2543], [97743, 9536, 7931, 4676, 4241, 4170, 3134, 14928], [96680, 11652, 8196, 5659, 5050, 12369, 1410, 5343]], "External outlinks": [[124177, 5575, 4657, 2262, 1982, 1801, 1360, 4545], [123897, 5914, 4529, 2418, 1971, 1870, 1183, 4577], [123044, 5922, 5201, 2389, 1995, 1924, 1299, 4585], [124164, 5520, 4671, 2108, 2016, 1900, 1187, 4793], [124233, 5758, 4319, 2136, 1981, 1691, 1340, 4901], [124820, 5418, 4485, 1896, 2011, 1720, 1116, 4893], [124990, 5065, 4422, 1994, 1961, 1764, 1158, 5005], [125322, 4975, 4418, 1991, 1821, 1470, 1165, 5197], [122524, 3893, 6861, 3338, 2457, 1559, 4040, 1687], [128012, 6794, 6252, 945, 901, 660, 1114, 1681], [125097, 4822, 4273, 2070, 2027, 1829, 1167, 5074], [124488, 6155, 4642, 2336, 2016, 3422, 610, 2690]]}
cc = {"Page text": [[[71871, 5502, 2999, 692, 415, 116, 47, 15], [5040, 2832, 1644, 890, 514, 225, 74, 38], [2842, 1579, 2308, 1267, 1036, 514, 200, 110], [612, 770, 1288, 1266, 1044, 834, 380, 274], [416, 389, 943, 1080, 1237, 967, 597, 404], [120, 229, 458, 733, 898, 1465, 817, 1838], [33, 73, 191, 352, 563, 868, 1190, 1150], [7, 43, 118, 247, 414, 1754, 1116, 16411]], [[70237, 5895, 3379, 825, 438, 111, 51, 5], [5322, 2777, 1597, 892, 464, 248, 94, 23], [2991, 1487, 2336, 1344, 1043, 468, 195, 85], [786, 842, 1239, 1316, 1030, 737, 321, 256], [394, 443, 1004, 1077, 1141, 985, 687, 390], [88, 213, 467, 697, 1012, 1483, 969, 1814], [53, 78, 214, 289, 533, 859, 1228, 1167], [7, 40, 70, 226, 435, 1655, 1226, 16581]], [[71038, 4894, 2728, 566, 459, 148, 37, 8], [5925, 2776, 1581, 778, 403, 200, 74, 38], [3250, 1688, 2430, 1229, 916, 514, 181, 98], [717, 981, 1367, 1310, 1009, 709, 324, 249], [435, 493, 1014, 975, 1235, 915, 572, 457], [122, 208, 498, 751, 936, 1415, 857, 1759], [91, 86, 220, 356, 622, 844, 1229, 1323], [14, 44, 144, 266, 418, 1800, 1191, 16444]], [[72292, 5126, 2887, 660, 419, 127, 53, 28], [5130, 2899, 1511, 852, 458, 204, 62, 54], [3142, 1571, 2360, 1184, 1012, 418, 165, 130], [806, 856, 1173, 1157, 977, 717, 313, 232], [393, 473, 1035, 1122, 1152, 940, 536, 347], [139, 216, 557, 847, 951, 1389, 777, 1669], [49, 71, 205, 369, 592, 837, 1296, 1046], [7, 39, 80, 244, 424, 1577, 1171, 16834]], [[73542, 4489, 2803, 566, 395, 101, 54, 8], [5869, 2602, 1422, 677, 375, 202, 67, 37], [3367, 1465, 2182, 1167, 856, 447, 232, 92], [769, 985, 1243, 1260, 903, 712, 332, 231], [491, 576, 982, 952, 1154, 901, 574, 355], [179, 246, 465, 698, 926, 1374, 762, 1559], [27, 74, 201, 321, 557, 956, 1142, 1095], [22, 35, 75, 210, 344, 1486, 1155, 17013]], [[74827, 5219, 2954, 623, 437, 134, 55, 17], [5110, 2346, 1474, 754, 476, 231, 52, 29], [2993, 1551, 2143, 1122, 895, 426, 159, 84], [665, 846, 1183, 1094, 908, 619, 332, 204], [516, 417, 912, 906, 1056, 879, 507, 317], [192, 250, 476, 730, 900, 1336, 793, 1502], [42, 77, 207, 313, 596, 882, 1177, 1024], [9, 28, 83, 254, 336, 1458, 1082, 17140]], [[75470, 4555, 2978, 627, 461, 141, 112, 10], [5114, 2440, 1503, 916, 424, 210, 91, 36], [3121, 1267, 2322, 1101, 858, 423, 209, 131], [637, 735, 1133, 1143, 897, 689, 324, 238], [393, 402, 995, 985, 1054, 853, 554, 368], [94, 209, 437, 687, 814, 1264, 797, 1663], [34, 83, 191, 276, 490, 776, 1120, 1187], [9, 29, 74, 172, 271, 1185, 1173, 17404]], [[73299, 2906, 5577, 2207, 670, 170, 41, 2], [4188, 1512, 1735, 1369, 583, 264, 65, 4], [2794, 1039, 2407, 1538, 1145, 518, 171, 21], [547, 541, 1090, 1435, 1161, 680, 410, 43], [386, 309, 901, 1046, 1141, 879, 526, 81], [105, 179, 442, 726, 1137, 1262, 1456, 234], [51, 68, 222, 423, 707, 1201, 1419, 289], [12, 55, 129, 431, 1198, 1175, 14268, 3769]], [[75950, 3273, 1548, 314, 203, 57, 35, 2], [3571, 1552, 812, 339, 193, 97, 36, 9], [4974, 4208, 1905, 622, 461, 214, 94, 25], [2986, 2553, 1878, 670, 528, 335, 181, 44], [1338, 1901, 2430, 532, 604, 432, 412, 93], [463, 1303, 2384, 332, 446, 510, 438, 273], [257, 1347, 12802, 161, 359, 468, 2659, 303], [3, 12, 23, 37, 63, 274, 299, 3732]], [[76087, 6273, 4010, 1328, 1007, 428, 222, 187], [4824, 2488, 2419, 1791, 1515, 1246, 764, 1102], [1644, 1027, 1714, 1276, 1586, 2284, 1727, 12524], [292, 365, 582, 602, 511, 370, 178, 107], [186, 153, 437, 472, 537, 462, 323, 287], [48, 118, 201, 270, 350, 486, 444, 470], [17, 36, 82, 129, 220, 432, 481, 2757], [3, 10, 31, 45, 82, 247, 286, 3777]], [[73599, 5540, 2769, 706, 339, 102, 35, 11], [4750, 2756, 1497, 829, 389, 177, 51, 21], [2842, 1942, 2126, 1143, 859, 373, 139, 52], [666, 1028, 1227, 1240, 869, 592, 177, 114], [504, 623, 1219, 1140, 1053, 815, 285, 169], [112, 317, 667, 1005, 1050, 1783, 415, 606], [49, 145, 323, 558, 1038, 1164, 571, 577], [12, 72, 152, 684, 1063, 11509, 519, 7200]]], "Internal outlinks": [[[87279, 5425, 2456, 595, 283, 79, 36, 10], [5035, 2632, 1344, 713, 380, 158, 49, 23], [2491, 1354, 1735, 1007, 784, 393, 147, 62], [585, 637, 958, 996, 763, 574, 257, 171], [308, 300, 810, 821, 1019, 742, 481, 297], [83, 150, 349, 546, 759, 1092, 603, 1283], [17, 50, 118, 250, 420, 739, 808, 919], [5, 24, 66, 169, 272, 1221, 690, 11537]], [[86557, 5471, 2697, 658, 303, 92, 24, 1], [5149, 2708, 1438, 662, 375, 167, 54, 19], [2458, 1390, 1770, 980, 738, 319, 122, 59], [621, 707, 987, 964, 862, 535, 259, 162], [283, 376, 770, 783, 844, 799, 537, 288], [66, 183, 333, 529, 749, 1156, 737, 1245], [41, 58, 177, 222, 385, 595, 811, 782], [5, 18, 46, 170, 267, 1209, 837, 11750]], [[86929, 4872, 2412, 471, 356, 100, 35, 5], [5697, 2602, 1358, 670, 343, 170, 53, 18], [2875, 1376, 1770, 907, 721, 375, 134, 60], [613, 757, 937, 915, 760, 554, 262, 170], [303, 377, 765, 770, 911, 726, 371, 300], [112, 182, 320, 541, 702, 1158, 649, 1208], [78, 67, 147, 285, 470, 578, 835, 921], [8, 26, 58, 179, 264, 1138, 900, 11733]], [[87744, 5245, 2686, 494, 292, 76, 59, 19], [4855, 2750, 1305, 762, 379, 135, 48, 25], [2512, 1446, 1731, 921, 674, 301, 131, 51], [554, 718, 914, 913, 719, 533, 224, 163], [351, 348, 825, 818, 899, 631, 363, 292], [73, 167, 419, 681, 720, 1036, 575, 1128], [35, 56, 133, 252, 457, 659, 895, 752], [4, 19, 48, 158, 249, 1151, 850, 11936]], [[88472, 4396, 2434, 418, 296, 74, 32, 6], [5740, 2671, 1232, 604, 296, 140, 46, 20], [2872, 1385, 1696, 930, 677, 304, 151, 46], [633, 813, 962, 1041, 710, 460, 222, 158], [356, 380, 764, 717, 777, 672, 480, 243], [96, 181, 325, 539, 663, 1041, 597, 1080], [20, 49, 140, 251, 399, 657, 855, 774], [5, 22, 41, 171, 276, 1027, 830, 11994]], [[90076, 4601, 2555, 495, 353, 71, 37, 6], [5074, 2404, 1272, 595, 301, 178, 49, 24], [2591, 1388, 1578, 894, 702, 284, 112, 45], [554, 753, 862, 888, 751, 484, 227, 152], [365, 376, 676, 718, 748, 632, 364, 215], [87, 143, 359, 515, 655, 987, 600, 1029], [21, 54, 142, 248, 474, 650, 781, 843], [8, 20, 53, 158, 213, 982, 902, 11985]], [[91175, 4189, 2413, 496, 337, 112, 45, 9], [4733, 2471, 1332, 717, 273, 138, 48, 27], [2580, 1192, 1739, 827, 651, 284, 126, 98], [579, 646, 844, 885, 690, 529, 188, 150], [359, 313, 759, 755, 759, 632, 365, 255], [99, 165, 306, 457, 588, 947, 624, 1082], [56, 57, 128, 176, 315, 588, 781, 971], [2, 18, 48, 132, 170, 899, 748, 12282]], [[89027, 3005, 4734, 2123, 512, 154, 27, 1], [3922, 1424, 1694, 1271, 506, 189, 42, 3], [2408, 756, 1828, 1285, 829, 332, 121, 10], [431, 446, 843, 1077, 877, 501, 251, 19], [264, 206, 630, 821, 844, 644, 337, 37], [73, 132, 315, 531, 825, 1039, 1092, 122], [47, 49, 117, 255, 482, 802, 1018, 155], [6, 42, 92, 319, 869, 786, 10632, 2128]], [[91584, 2870, 1239, 254, 162, 50, 17, 2], [3526, 1382, 660, 266, 138, 63, 19, 6], [4091, 3585, 1531, 481, 359, 137, 64, 5], [2830, 2146, 1433, 478, 367, 274, 113, 41], [1065, 1449, 1852, 389, 396, 290, 253, 50], [322, 1042, 1775, 230, 274, 351, 286, 167], [165, 1073, 9722, 107, 240, 298, 1755, 160], [2, 7, 9, 13, 42, 127, 163, 2112]], [[91737, 5892, 3539, 1032, 809, 319, 131, 126], [4295, 2266, 2055, 1502, 1130, 916, 577, 813], [1310, 835, 1381, 1048, 1119, 1616, 1376, 9536], [218, 313, 438, 456, 351, 254, 124, 64], [135, 115, 322, 321, 378, 315, 193, 199], [35, 87, 133, 196, 269, 329, 268, 273], [12, 22, 51, 95, 134, 274, 286, 1796], [1, 6, 12, 26, 51, 147, 179, 2121]], [[88576, 5726, 2505, 572, 251, 72, 33, 8], [4431, 2605, 1287, 707, 309, 152, 35, 10], [2645, 1600, 1769, 917, 631, 256, 86, 27], [530, 846, 1014, 921, 704, 456, 138, 67], [374, 510, 836, 894, 781, 551, 182, 113], [79, 215, 448, 767, 781, 1260, 275, 345], [35, 97, 217, 405, 789, 872, 317, 402], [10, 53, 120, 476, 804, 8750, 344, 4371]]], "External outlinks": [[[118253, 3430, 1938, 280, 180, 68, 13, 15], [3240, 1107, 643, 342, 139, 81, 18, 5], [1931, 610, 965, 504, 390, 173, 66, 18], [236, 362, 401, 510, 349, 243, 97, 64], [188, 159, 357, 368, 350, 268, 168, 124], [28, 102, 141, 213, 272, 404, 201, 440], [18, 121, 65, 110, 180, 210, 330, 326], [3, 23, 19, 91, 111, 423, 290, 3585]], [[117279, 3651, 2358, 351, 194, 47, 14, 3], [3305, 1151, 691, 344, 175, 208, 34, 6], [1964, 538, 997, 443, 361, 154, 56, 16], [267, 338, 478, 506, 378, 279, 93, 79], [181, 141, 339, 348, 389, 293, 183, 97], [34, 69, 194, 225, 276, 380, 211, 481], [12, 24, 72, 103, 145, 176, 362, 289], [2, 10, 72, 69, 77, 387, 346, 3614]], [[117546, 3034, 1961, 249, 199, 38, 12, 5], [3667, 1202, 497, 261, 163, 107, 18, 7], [2409, 636, 1117, 456, 324, 157, 56, 46], [283, 383, 490, 399, 397, 254, 91, 92], [209, 155, 386, 344, 426, 222, 134, 119], [33, 83, 157, 249, 270, 501, 206, 425], [15, 23, 45, 79, 165, 205, 355, 412], [2, 4, 18, 71, 72, 416, 315, 3687]], [[118222, 3507, 1912, 272, 183, 43, 21, 4], [3273, 1122, 539, 337, 166, 58, 15, 10], [2184, 538, 957, 399, 360, 143, 72, 18], [250, 342, 370, 408, 345, 213, 86, 94], [203, 155, 347, 359, 394, 284, 174, 100], [85, 66, 125, 240, 287, 359, 296, 442], [14, 21, 49, 81, 161, 207, 377, 277], [2, 7, 20, 40, 85, 384, 299, 3956]], [[118751, 3023, 1962, 231, 206, 39, 19, 2], [3526, 1183, 556, 242, 149, 59, 27, 16], [1960, 531, 948, 317, 341, 151, 52, 19], [271, 390, 400, 411, 284, 249, 78, 53], [201, 191, 363, 331, 395, 262, 135, 103], [82, 75, 162, 207, 258, 366, 180, 361], [28, 20, 64, 92, 301, 232, 322, 281], [1, 5, 30, 65, 77, 362, 303, 4058]], [[119384, 2918, 2003, 259, 189, 49, 15, 3], [3226, 1109, 516, 303, 163, 83, 13, 5], [1882, 551, 1006, 400, 363, 193, 68, 22], [219, 248, 373, 360, 336, 209, 78, 73], [181, 134, 332, 305, 400, 292, 182, 185], [73, 73, 132, 210, 292, 364, 206, 370], [20, 18, 46, 103, 140, 258, 291, 240], [5, 14, 14, 54, 78, 316, 305, 4107]], [[119703, 2709, 2086, 207, 207, 50, 27, 1], [2945, 1094, 466, 288, 177, 70, 18, 7], [1994, 556, 964, 379, 303, 150, 56, 20], [236, 317, 348, 405, 336, 205, 91, 56], [223, 186, 357, 333, 359, 273, 144, 86], [77, 87, 125, 245, 221, 313, 219, 477], [72, 18, 46, 84, 135, 178, 302, 323], [72, 8, 26, 50, 83, 231, 308, 4227]], [[117600, 2161, 3981, 1235, 253, 79, 13, 0], [2482, 769, 983, 424, 220, 74, 22, 1], [1937, 505, 908, 431, 458, 130, 43, 6], [244, 215, 416, 447, 366, 186, 101, 16], [205, 140, 312, 361, 381, 253, 152, 17], [39, 74, 150, 242, 305, 267, 324, 69], [14, 19, 79, 95, 194, 255, 422, 87], [3, 10, 32, 103, 280, 315, 2963, 1491]], [[119593, 1916, 796, 113, 75, 19, 11, 1], [2686, 721, 254, 121, 71, 22, 14, 4], [3572, 2133, 709, 196, 158, 60, 27, 6], [1455, 755, 584, 195, 171, 96, 63, 19], [523, 610, 693, 176, 180, 129, 124, 22], [137, 345, 527, 89, 119, 132, 120, 90], [45, 312, 2683, 47, 107, 99, 671, 76], [1, 2, 6, 8, 20, 103, 84, 1463]], [[120981, 3375, 2393, 577, 415, 170, 71, 30], [3071, 900, 879, 614, 549, 366, 179, 236], [839, 340, 580, 404, 532, 679, 504, 2374], [101, 122, 184, 178, 172, 114, 43, 31], [80, 49, 146, 166, 176, 121, 85, 78], [18, 21, 48, 77, 107, 155, 98, 136], [6, 8, 38, 42, 59, 130, 125, 706], [1, 7, 5, 12, 17, 94, 62, 1483]], [[119166, 3533, 1909, 307, 142, 24, 9, 7], [2759, 1093, 531, 244, 131, 44, 17, 3], [1900, 662, 930, 386, 252, 98, 34, 11], [322, 372, 460, 413, 296, 134, 43, 30], [257, 294, 436, 356, 356, 195, 92, 41], [50, 138, 219, 308, 374, 445, 131, 164], [27, 49, 96, 190, 240, 288, 123, 154], [7, 14, 61, 132, 225, 2194, 161, 2280]]]}
cpd = {"Page text": [35517, 36536, 41080, 40607, 40406, 40404, 39722, 35830, 37177, 41503, 40821, 40718, 41221, 40241, 35620, 36868, 40867, 41536, 42151, 42596, 40346, 36008, 37019, 40632, 41235, 40417, 40749, 39970, 35944, 36229, 40876, 41319, 40701, 40450, 39061, 35139, 35988, 40336, 40121, 39292, 39515, 38382, 34710, 35373, 39088, 39579, 38491, 39016, 38727, 36114, 36584, 39631, 39805, 39257, 39324, 38812, 35614, 36400, 51365, 49689, 38479, 38961, 38167, 9029, 9127, 40715, 16021, 16093, 40210, 15834, 14388, 36813, 40336, 40239, 40167, 40583, 40240, 36817, 38237, 18979, 19395, 41690, 42141, 40719, 36754, 38284, 42698, 41940, 41108], "Internal outlinks": [25718, 26050, 30383, 30795, 29985, 30026, 29500, 25615, 26525, 30684, 30532, 30369, 30427, 29786, 25462, 26533, 30024, 30649, 31326, 31232, 29560, 25807, 26434, 30327, 30399, 30023, 30313, 29296, 25657, 25959, 30309, 30849, 30429, 29853, 28733, 25334, 25799, 30038, 29626, 29101, 29399, 28131, 24780, 25506, 28901, 29144, 28600, 28670, 28188, 25910, 26010, 29388, 28790, 28669, 28840, 27886, 25386, 26126, 39547, 37849, 28270, 28432, 27507, 5537, 5685, 29897, 10757, 10751, 29384, 10613, 9246, 26417, 29466, 29599, 29618, 29940, 29253, 26247, 27702, 13045, 13462, 30973, 31147, 30170, 26428, 27536, 31526, 31250, 30774], "External outlinks": [10077, 10570, 11804, 11673, 11478, 11768, 11373, 9917, 10560, 11733, 11739, 11745, 11518, 11351, 9951, 10732, 11899, 12017, 12282, 12196, 11735, 10119, 10663, 11483, 11946, 11776, 12064, 11437, 10054, 10559, 11780, 12110, 11762, 11849, 11353, 10117, 10353, 11671, 11533, 11479, 11594, 11045, 9992, 10308, 11450, 11754, 11215, 11692, 11496, 10623, 10391, 11385, 11791, 11306, 11642, 11014, 10258, 10724, 17488, 16461, 11076, 11278, 11021, 3253, 3209, 12189, 5280, 5221, 11584, 5232, 4773, 10762, 11849, 11579, 11521, 11766, 11354, 10520, 10818, 6066, 6248, 12081, 12444, 11866, 10588, 10935, 12350, 12412, 12088]}

draw_alluvial_plot(cc["Page text"])
# draw_alluvial_plot(cc["Internal outlinks"])
# draw_alluvial_plot(cc["External outlinks"])

draw_page_changes_per_day(cpd["Page text"])
draw_different_change_behaviour_fractions(cc["Page text"])
draw_same_change_behaviour_fractions(cc["Page text"])
draw_fraction_not_changed_weekly(cm["Page text"])
draw_fraction_changed_every_day_weekly(cm["Page text"])

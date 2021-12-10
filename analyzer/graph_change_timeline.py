"""
Read binary change data and compute binary change statistics.
Author: Daan Kooij
Last modified: December 10th, 2021
"""

from collections import defaultdict
from datetime import datetime

import matplotlib.pyplot as plt
import numpy as np
import plotly.express as px
import plotly.graph_objects as go


INPUT_FILE_PATH = "inputmisc/change-data-combined.csv"
NUMBER_OF_DAY_PAIRS = 89
FIRST_WEEK = 24
LAST_WEEK = 35


# COMMON DATA COMPUTATION FUNCTIONS

def compute_change_dicts(only_first_stage=False):
    # change_dict: {change_type -> {page_id -> {week -> number_of_changes}}}
    change_dicts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    with open(INPUT_FILE_PATH) as input_file:
        for line in input_file:
            [page_id, date_str, text_change_str, ilink_change_str, elink_change_str] = line.strip().split(",")
            if not only_first_stage or page_id.split("-")[0] == "s00":
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


def compute_page_changes_per_day(only_first_stage=False, limit_weekday=-1):
    changes_per_day, start_date = defaultdict(list), None

    with open(INPUT_FILE_PATH) as input_file:
        for line in input_file:
            [page_id, date_str, text_change_str, ilink_change_str, elink_change_str] = line.strip().split(",")
            if not only_first_stage or page_id.split("-")[0] == "s00":
                date_obj = datetime.strptime(date_str, "%Y%m%d")

                if start_date is None:
                    start_date = date_obj
                day_number = (date_obj - start_date).days

                if limit_weekday != -1:
                    weekday = (day_number - 2) % 7  # 0-6: Monday-Sunday
                    if weekday == limit_weekday:
                        day_number = int(day_number / 7)
                    else:
                        continue  # Skip this day, go to next loop iteration

                while day_number >= len(changes_per_day["Page text"]):
                    changes_per_day["Page text"].append(0)
                    changes_per_day["Internal outlinks"].append(0)
                    changes_per_day["External outlinks"].append(0)
                changes_per_day["Page text"][day_number] += int(text_change_str)
                changes_per_day["Internal outlinks"][day_number] += int(ilink_change_str)
                changes_per_day["External outlinks"][day_number] += int(elink_change_str)

    return changes_per_day


def compute_changes_per_page(only_first_stage=False):
    changes_per_page, pages_changed_per_day = defaultdict(int), defaultdict(set)
    with open(INPUT_FILE_PATH) as file:
        for line in file:
            [page_id, day, change_value] = line.split(",")[:3]
            if not only_first_stage or page_id.split("-")[0] == "s00":
                if bool(int(change_value)):
                    changes_per_page[page_id] += 1
                    pages_changed_per_day[day].add(page_id)
    return changes_per_page, pages_changed_per_day


# DATA TRANSFORMATION COMPUTATION FUNCTIONS

def compute_rare_changes(changes_per_page, pages_changed_per_day, threshold_fraction=0.1):
    rare_page_changes = set(page_id for page_id, changes in changes_per_page.items()
                            if changes < NUMBER_OF_DAY_PAIRS * threshold_fraction)
    rare_changes_per_day = defaultdict(int)
    for day, pages_changed in pages_changed_per_day.items():
        for page_id in pages_changed:
            if page_id in rare_page_changes:
                rare_changes_per_day[day] += 1
    return [v for _, v in sorted(rare_changes_per_day.items())]


def compute_day_change_fractions(changes_per_page):
    return [c / NUMBER_OF_DAY_PAIRS for c in sorted(list(changes_per_page.values()), reverse=True)]


def compute_average_change_behaviour(change_matrices):
    num_pages = sum(change_matrices["Page text"][0])
    result_dict = dict()
    for feature_id, change_matrix in change_matrices.items():
        result_dict[feature_id] = [0 for _ in range(len(change_matrix[0]))]
        for change_list in change_matrix:
            for index, change_value in zip(range(len(change_list)), change_list):
                result_dict[feature_id][index] += change_value
        result_dict[feature_id] = [value / (len(change_matrix) * num_pages) for value in result_dict[feature_id]]
    return result_dict


def normalize_results(cm_dict, cc_dict, cpd_dict):
    new_cm_dict, new_cc_dict, new_cpd_dict = dict(), dict(), dict()
    number_of_pages = sum(cm_dict["Page text"][0])
    for key in cm_dict:
        change_matrix, change_cube, changes_per_day = cm_dict[key], cc_dict[key], cpd_dict[key]
        new_cm_dict[key] = [[cell / number_of_pages for cell in row] for row in change_matrix]
        new_cc_dict[key] = [[[cell / number_of_pages for cell in row] for row in matrix] for matrix in change_cube]
        new_cpd_dict[key] = [cell / number_of_pages for cell in changes_per_day]
    return new_cm_dict, new_cc_dict, new_cpd_dict


# DAILY CHANGE PLOT FUNCTIONS

def draw_page_changes_per_day(changes_per_day, plot_multiple=False, corrected_dataset=False):
    day_numbers = list(range(2, len(changes_per_day["Page text"]) + 2))

    plt.figure()
    plt.plot(day_numbers, changes_per_day["Page text"], label="Page text", linewidth=2.5, color=plt.cm.Dark2(0))
    if plot_multiple:
        plt.plot(day_numbers, changes_per_day["Internal outlinks"], label="Internal out-links",
                 linewidth=2.5, color=plt.cm.Dark2(1))
        plt.plot(day_numbers, changes_per_day["External outlinks"], label="External out-links",
                 linewidth=2.5, color=plt.cm.Dark2(2))
        title = "Fraction of pages per day for which a given feature changes\n"
        plt.legend()
    else:
        title = "Fraction of pages per day for which text changes "
    title += "(corrected dataset)" if corrected_dataset else "(full dataset)"
    plt.title(title)
    plt.xlabel("Day number")
    plt.ylabel("Fraction changed")
    plt.grid()
    plt.tight_layout()
    if plot_multiple:
        plt.savefig("figures/timeline/daily-changes-corrected.png", dpi=400)
    else:
        plt.savefig("figures/timeline/daily-changes-full.png", dpi=400)


def draw_rare_page_changes(rare_page_changes):
    plt.figure()
    plt.plot(list(range(2, len(rare_page_changes) + 2)), rare_page_changes, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Number of rare page changes per day")
    plt.xlabel("Day number")
    plt.ylabel("Rare page changes")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/daily-rare-changes.png", dpi=400)


def draw_day_change_fractions_percentile(page_change_counts):
    plt.figure()
    x = np.linspace(0, 100, num=len(page_change_counts))
    plt.plot(x, page_change_counts, linewidth=2.5, color=plt.cm.Dark2(0))
    plt.title("Percentile plot of day change fractions")
    plt.xlabel("Percentile")
    plt.ylabel("Fraction of days changed")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/daily-change-fractions-percentile.png", dpi=400)


def draw_page_changes_per_single_day(sunday_changes, tuesday_changes):
    day_numbers = list(range(1, len(sunday_changes["Page text"]) + 1))

    plt.figure()
    plt.plot(day_numbers, tuesday_changes["Page text"], label="Page text (Tuesday)",
             linewidth=2.5, color=plt.cm.Dark2(0))
    plt.plot(day_numbers, sunday_changes["Page text"], label="Page text (Sunday)",
             linewidth=2.5, color=plt.cm.Dark2(1))
    plt.plot(day_numbers, tuesday_changes["Internal outlinks"], label="Internal out-links (Tuesday)",
             linewidth=2.5, color=plt.cm.Dark2(2))
    plt.plot(day_numbers, sunday_changes["Internal outlinks"], label="Internal out-links (Sunday)",
             linewidth=2.5, color=plt.cm.Dark2(3))
    plt.plot(day_numbers, tuesday_changes["External outlinks"], label="External out-links (Tuesday)",
             linewidth=2.5, color=plt.cm.Dark2(4))
    plt.plot(day_numbers, sunday_changes["External outlinks"], label="External out-links (Sunday)",
             linewidth=2.5, color=plt.cm.Dark2(5))
    plt.title("Fraction of pages per single day for which a given feature changes")
    plt.xlabel("Day occurrence")
    plt.ylabel("Fraction changed")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/daily-single-day-changes.png", dpi=400)


# WEEKLY CHANGE PLOT FUNCTIONS

def draw_alluvial_plot(change_cube, change_type):
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
                pad=12,
                thickness=36,
                line=dict(color="black", width=0.0),
                color=node_colors,
                label=node_labels
            ),
            link=dict(
                source=sources,  # indices correspond to node_labels, eg A1, A2, A1, B1, ...
                target=targets,
                value=values,
                color=link_colors
            )
        )],
        layout=dict(
            font=dict(size=24)
        ))

    fig.update_layout(title_text="Alluvial Plot depicting the number of days that " + change_type +
                                 " on pages in the Dutch Web changes per week, in weeks 24-35 of 2021")
    fig.show()


def draw_average_change_behaviour_fractions(average_change_behaviour, change_type="other"):
    fig = plt.figure(figsize=(6.4, 3.2 * (1.5 if change_type == "text" else 1)))
    if change_type == "text":
        change_behaviour_dict = {"Page text": average_change_behaviour["Page text"]}
    else:
        change_behaviour_dict = {"Internal outlinks": average_change_behaviour["Internal outlinks"],
                                 "External outlinks": average_change_behaviour["External outlinks"]}

    for plot_index, (feature_id, average_change_values) in zip(range(1, len(change_behaviour_dict) + 1),
                                                               change_behaviour_dict.items()):
        labels = [str(i) for i in range(len(average_change_values))]
        ax = fig.add_subplot(1, len(change_behaviour_dict), plot_index)
        ax.pie(average_change_values, normalize=True, startangle=90, counterclock=False,
                colors=plt.cm.Dark2.colors)
        ax.legend(labels)
        ax.set_title(feature_id)
    fig.suptitle("Average change behaviour across all weeks")
    fig.tight_layout()

    fig.savefig("figures/timeline/weekly-average-change-behaviour-" + change_type + ".png", dpi=400)


def draw_change_behaviour_per_week(change_matrix):
    num_behaviours = len(change_matrix[0])
    fig, (ax_top, ax_bot) = plt.subplots(2, 1, sharex=True)
    for j in range(num_behaviours):
        x = [FIRST_WEEK + i + j * 0.11 - 0.385 for i in range(len(change_matrix))]
        y = [change_matrix[i][j] for i in range(len(change_matrix))]
        ax_top.bar(x, y, width=0.11, label=str(j), color=plt.cm.Dark2(j))
        ax_bot.bar(x, y, width=0.11, color=plt.cm.Dark2(j))
    ax_top.set_ylim(0.54, 0.66)
    ax_bot.set_ylim(0, 0.12)
    ax_top.set_xlim(FIRST_WEEK - 1, FIRST_WEEK + 12.65)
    ax_bot.set_xlim(FIRST_WEEK - 1, FIRST_WEEK + 12.65)
    ax_top.spines["bottom"].set_visible(False)
    ax_bot.spines["top"].set_visible(False)
    ax_top.xaxis.tick_top()

    # Set interruption diagonal lines
    d = .015  # how big to make the diagonal lines in axes coordinates
    kwargs = dict(transform=ax_top.transAxes, color='k', clip_on=False)
    ax_top.plot((-d, +d), (-d, +d), **kwargs)  # top-left diagonal
    ax_top.plot((1 - d, 1 + d), (-d, +d), **kwargs)  # top-right diagonal
    kwargs.update(transform=ax_bot.transAxes)  # switch to the bottom axes
    ax_bot.plot((-d, +d), (1 - d, 1 + d), **kwargs)  # bottom-left diagonal
    ax_bot.plot((1 - d, 1 + d), (1 - d, 1 + d), **kwargs)  # bottom-right diagonal

    ax_top.set_title("Change behaviour fractions per week")
    ax_top.legend()
    plt.xlabel("Week number")
    plt.ylabel("Change behaviour fraction")
    ax_bot.grid(axis="y")
    ax_top.grid(axis="y")
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-change-behaviour.png", dpi=400)


def draw_same_change_behaviour_fractions(change_cube, limit_three_classes=False):
    # Percentage of pages that continue to exhibit same change behaviour
    # in following week, per change behaviour per week
    week_numbers = list(range(FIRST_WEEK + 1, LAST_WEEK + 1))

    if limit_three_classes:
        fractions_per_change_behaviour = [[] for _ in range(3)]
        labels = ["Never (0)", "Sometimes (1-6)", "Always (7)"]
        for change_matrix in change_cube:
            week_never_same, week_never_total = change_matrix[0][0], sum(change_matrix[0])
            week_sometimes_same, week_sometimes_total = 0, 0
            for i in range(1, 7):
                for j in range(1, 7):
                    week_sometimes_same += change_matrix[i][j]
                week_sometimes_total += sum(change_matrix[i])
            week_always_same, week_always_total = change_matrix[7][7], sum(change_matrix[7])

            fractions_per_change_behaviour[0].append(week_never_same / week_never_total)
            fractions_per_change_behaviour[1].append(week_sometimes_same / week_sometimes_total)
            fractions_per_change_behaviour[2].append(week_always_same / week_always_total)
    else:
        fractions_per_change_behaviour = [[] for _ in range(len(change_cube[0]))]
        labels = [str(i) for i in range(8)]
        for change_matrix in change_cube:
            for i in range(len(change_matrix)):
                week_cb_same = change_matrix[i][i]
                week_cb_total = sum(change_matrix[i])
                fractions_per_change_behaviour[i].append(week_cb_same / week_cb_total)

    plt.figure()
    for i, fractions_list in zip(range(len(fractions_per_change_behaviour)), fractions_per_change_behaviour):
        color = plt.cm.Dark2(7) if limit_three_classes and i == 2 else plt.cm.Dark2(i)
        plt.plot(week_numbers, fractions_list, linewidth=2.5, color=color, label=labels[i])
    if limit_three_classes:
        title = "Fraction of pages that continue to exhibit same change\n" \
                "behaviour as previous week (reduced to three classes)"
    else:
        title = "Fraction of pages that continue to exhibit\n" \
                "same change behaviour as previous week"
    plt.title(title)
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-same-change-behaviour" +
                ("-three-classes" if limit_three_classes else "") + ".png", dpi=400)


def draw_same_change_behaviour_fractions_combined(change_cube, limit_three_classes=False):
    week_numbers = list(range(FIRST_WEEK + 1, LAST_WEEK + 1))

    if limit_three_classes:
        fractions = []
        for change_matrix in change_cube:
            week_same = change_matrix[0][0] + change_matrix[7][7]
            week_total = sum(change_matrix[0]) + sum(change_matrix[7])
            for i in range(1, 7):
                for j in range(1, 7):
                    week_same += change_matrix[i][j]
                week_total += sum(change_matrix[i])
            fractions.append(week_same / week_total)
    else:
        fractions = []  # Percentage of pages that exhibit same change behaviour in following week, per week
        for change_matrix in change_cube:
            week_same, total = 0, 0
            for i in range(len(change_matrix)):
                week_same += change_matrix[i][i]
                total += sum(change_matrix[i])
            fractions.append(week_same / total)

    plt.figure()
    plt.plot(week_numbers, fractions, linewidth=2.5, color=plt.cm.Dark2(0))
    if limit_three_classes:
        title = "Fraction of pages that exhibit same change\n" \
                "behaviour as previous week with change behaviours\n" \
                "combined, reduced to three classes)"
    else:
        title = "Fraction of pages that exhibit same change behaviour\n" \
                "as previous week (with change behaviours combined)"
    plt.title(title)
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-same-change-behaviour-combined" +
                ("-three-classes" if limit_three_classes else "") + ".png", dpi=400)


def draw_neighbouring_change_behaviour_fractions(change_cube, limit_three_classes=False):
    week_numbers = list(range(FIRST_WEEK + 1, LAST_WEEK + 1))

    if limit_three_classes:
        fractions_per_change_behaviour = [[] for _ in range(3)]
        labels = ["Never (0)", "Sometimes (1-6)", "Always (7)"]
        for change_matrix in change_cube:
            week_never_total = sum(change_matrix[0])
            week_never_neighbouring = week_never_total - change_matrix[0][0] - change_matrix[0][7]
            week_sometimes_neighbouring, week_sometimes_total = 0, 0
            for i in range(1, 7):
                week_sometimes_neighbouring += change_matrix[i][0]
                week_sometimes_neighbouring += change_matrix[i][7]
                week_sometimes_total += sum(change_matrix[i])
            week_always_total = sum(change_matrix[7])
            week_always_neighbouring = week_always_total - change_matrix[7][0] - change_matrix[7][7]

            fractions_per_change_behaviour[0].append(week_never_neighbouring / week_never_total)
            fractions_per_change_behaviour[1].append(week_sometimes_neighbouring / week_sometimes_total)
            fractions_per_change_behaviour[2].append(week_always_neighbouring / week_always_total)
    else:
        fractions_per_change_behaviour = [[] for _ in range(len(change_cube[0]))]
        labels = [str(i) for i in range(8)]
        for change_matrix in change_cube:
            for i in range(len(change_matrix)):
                week_neighbouring = 0
                week_cb_total = sum(change_matrix[i])
                if i > 0:
                    week_neighbouring += change_matrix[i][i - 1]
                if i < len(change_matrix[i]) - 1:
                    week_neighbouring += change_matrix[i][i + 1]
                fractions_per_change_behaviour[i].append(week_neighbouring / week_cb_total)

    plt.figure()
    for i, fractions_list in zip(range(len(fractions_per_change_behaviour)), fractions_per_change_behaviour):
        color = plt.cm.Dark2(7) if limit_three_classes and i == 2 else plt.cm.Dark2(i)
        plt.plot(week_numbers, fractions_list, linewidth=2.5, color=color, label=labels[i])
    if limit_three_classes:
        title = "Fraction of pages that exhibit neighbouring change behaviour\n" \
                "compared to previous week (reduced to three classes)"
    else:
        title = "Fraction of pages that exhibit neighbouring\n" \
                "change behaviour compared to previous week"
    plt.title(title)
    plt.xlabel("Week number")
    plt.ylabel("Fraction")
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.savefig("figures/timeline/weekly-neighbouring-change-behaviour" +
                ("-three-classes" if limit_three_classes else "") + ".png", dpi=400)


# DATA COMPUTATION FUNCTION INVOCATIONS
# cd = compute_change_dicts()
# cm = compute_changes_per_week(cd)
# cc = compute_change_cube(cd)
# cpd = compute_page_changes_per_day()

# cpd0 = compute_page_changes_per_day(only_first_stage=True)
# cpd0_sun = compute_page_changes_per_day(only_first_stage=True, limit_weekday=6)
# cpd0_tue = compute_page_changes_per_day(only_first_stage=True, limit_weekday=2)

# cd0 = compute_change_dicts(only_first_stage=True)
# cm0 = compute_changes_per_week(cd0)
# cc0 = compute_change_cube(cd0)

cm = {"Page text": [[81657, 11257, 9856, 6468, 6033, 6558, 4420, 20110], [80941, 11417, 9949, 6527, 6121, 6743, 4421, 20240], [79878, 11775, 10306, 6666, 6096, 6546, 4771, 20321], [81592, 11170, 9982, 6231, 5998, 6545, 4465, 20376], [81958, 11251, 9808, 6435, 5985, 6209, 4373, 20340], [84266, 10472, 9373, 5851, 5510, 6179, 4318, 20390], [84354, 10734, 9432, 5796, 5604, 5965, 4157, 20317], [84872, 9720, 9633, 5907, 5269, 5541, 4380, 21037], [81382, 6609, 12503, 9175, 7742, 6149, 18356, 4443], [89542, 16149, 23782, 3007, 2857, 2387, 4154, 4481], [83101, 10470, 9476, 5913, 5808, 5955, 4425, 21211], [82534, 12423, 9980, 7305, 6660, 16515, 2192, 8750]], "Internal outlinks": [[96163, 10334, 7973, 4941, 4778, 4865, 3321, 13984], [95803, 10572, 7836, 5097, 4680, 4998, 3071, 14302], [95180, 10911, 8218, 4968, 4523, 4872, 3381, 14306], [96615, 10259, 7767, 4738, 4527, 4799, 3239, 14415], [96128, 10749, 8061, 4999, 4389, 4522, 3145, 14366], [98194, 9897, 7594, 4671, 4094, 4375, 3213, 14321], [98776, 9739, 7497, 4511, 4197, 4268, 3072, 14299], [99583, 9051, 7569, 4445, 3783, 4129, 2925, 14874], [96178, 6060, 10253, 7682, 5744, 4447, 13520, 2475], [103585, 13554, 18221, 2218, 1978, 1590, 2670, 2543], [97743, 9536, 7931, 4676, 4241, 4170, 3134, 14928], [96680, 11652, 8196, 5659, 5050, 12369, 1410, 5343]], "External outlinks": [[124177, 5575, 4657, 2262, 1982, 1801, 1360, 4545], [123897, 5914, 4529, 2418, 1971, 1870, 1183, 4577], [123044, 5922, 5201, 2389, 1995, 1924, 1299, 4585], [124164, 5520, 4671, 2108, 2016, 1900, 1187, 4793], [124233, 5758, 4319, 2136, 1981, 1691, 1340, 4901], [124820, 5418, 4485, 1896, 2011, 1720, 1116, 4893], [124990, 5065, 4422, 1994, 1961, 1764, 1158, 5005], [125322, 4975, 4418, 1991, 1821, 1470, 1165, 5197], [122524, 3893, 6861, 3338, 2457, 1559, 4040, 1687], [128012, 6794, 6252, 945, 901, 660, 1114, 1681], [125097, 4822, 4273, 2070, 2027, 1829, 1167, 5074], [124488, 6155, 4642, 2336, 2016, 3422, 610, 2690]]}
cc = {"Page text": [[[71871, 5502, 2999, 692, 415, 116, 47, 15], [5040, 2832, 1644, 890, 514, 225, 74, 38], [2842, 1579, 2308, 1267, 1036, 514, 200, 110], [612, 770, 1288, 1266, 1044, 834, 380, 274], [416, 389, 943, 1080, 1237, 967, 597, 404], [120, 229, 458, 733, 898, 1465, 817, 1838], [33, 73, 191, 352, 563, 868, 1190, 1150], [7, 43, 118, 247, 414, 1754, 1116, 16411]], [[70237, 5895, 3379, 825, 438, 111, 51, 5], [5322, 2777, 1597, 892, 464, 248, 94, 23], [2991, 1487, 2336, 1344, 1043, 468, 195, 85], [786, 842, 1239, 1316, 1030, 737, 321, 256], [394, 443, 1004, 1077, 1141, 985, 687, 390], [88, 213, 467, 697, 1012, 1483, 969, 1814], [53, 78, 214, 289, 533, 859, 1228, 1167], [7, 40, 70, 226, 435, 1655, 1226, 16581]], [[71038, 4894, 2728, 566, 459, 148, 37, 8], [5925, 2776, 1581, 778, 403, 200, 74, 38], [3250, 1688, 2430, 1229, 916, 514, 181, 98], [717, 981, 1367, 1310, 1009, 709, 324, 249], [435, 493, 1014, 975, 1235, 915, 572, 457], [122, 208, 498, 751, 936, 1415, 857, 1759], [91, 86, 220, 356, 622, 844, 1229, 1323], [14, 44, 144, 266, 418, 1800, 1191, 16444]], [[72292, 5126, 2887, 660, 419, 127, 53, 28], [5130, 2899, 1511, 852, 458, 204, 62, 54], [3142, 1571, 2360, 1184, 1012, 418, 165, 130], [806, 856, 1173, 1157, 977, 717, 313, 232], [393, 473, 1035, 1122, 1152, 940, 536, 347], [139, 216, 557, 847, 951, 1389, 777, 1669], [49, 71, 205, 369, 592, 837, 1296, 1046], [7, 39, 80, 244, 424, 1577, 1171, 16834]], [[73542, 4489, 2803, 566, 395, 101, 54, 8], [5869, 2602, 1422, 677, 375, 202, 67, 37], [3367, 1465, 2182, 1167, 856, 447, 232, 92], [769, 985, 1243, 1260, 903, 712, 332, 231], [491, 576, 982, 952, 1154, 901, 574, 355], [179, 246, 465, 698, 926, 1374, 762, 1559], [27, 74, 201, 321, 557, 956, 1142, 1095], [22, 35, 75, 210, 344, 1486, 1155, 17013]], [[74827, 5219, 2954, 623, 437, 134, 55, 17], [5110, 2346, 1474, 754, 476, 231, 52, 29], [2993, 1551, 2143, 1122, 895, 426, 159, 84], [665, 846, 1183, 1094, 908, 619, 332, 204], [516, 417, 912, 906, 1056, 879, 507, 317], [192, 250, 476, 730, 900, 1336, 793, 1502], [42, 77, 207, 313, 596, 882, 1177, 1024], [9, 28, 83, 254, 336, 1458, 1082, 17140]], [[75470, 4555, 2978, 627, 461, 141, 112, 10], [5114, 2440, 1503, 916, 424, 210, 91, 36], [3121, 1267, 2322, 1101, 858, 423, 209, 131], [637, 735, 1133, 1143, 897, 689, 324, 238], [393, 402, 995, 985, 1054, 853, 554, 368], [94, 209, 437, 687, 814, 1264, 797, 1663], [34, 83, 191, 276, 490, 776, 1120, 1187], [9, 29, 74, 172, 271, 1185, 1173, 17404]], [[73299, 2906, 5577, 2207, 670, 170, 41, 2], [4188, 1512, 1735, 1369, 583, 264, 65, 4], [2794, 1039, 2407, 1538, 1145, 518, 171, 21], [547, 541, 1090, 1435, 1161, 680, 410, 43], [386, 309, 901, 1046, 1141, 879, 526, 81], [105, 179, 442, 726, 1137, 1262, 1456, 234], [51, 68, 222, 423, 707, 1201, 1419, 289], [12, 55, 129, 431, 1198, 1175, 14268, 3769]], [[75950, 3273, 1548, 314, 203, 57, 35, 2], [3571, 1552, 812, 339, 193, 97, 36, 9], [4974, 4208, 1905, 622, 461, 214, 94, 25], [2986, 2553, 1878, 670, 528, 335, 181, 44], [1338, 1901, 2430, 532, 604, 432, 412, 93], [463, 1303, 2384, 332, 446, 510, 438, 273], [257, 1347, 12802, 161, 359, 468, 2659, 303], [3, 12, 23, 37, 63, 274, 299, 3732]], [[76087, 6273, 4010, 1328, 1007, 428, 222, 187], [4824, 2488, 2419, 1791, 1515, 1246, 764, 1102], [1644, 1027, 1714, 1276, 1586, 2284, 1727, 12524], [292, 365, 582, 602, 511, 370, 178, 107], [186, 153, 437, 472, 537, 462, 323, 287], [48, 118, 201, 270, 350, 486, 444, 470], [17, 36, 82, 129, 220, 432, 481, 2757], [3, 10, 31, 45, 82, 247, 286, 3777]], [[73599, 5540, 2769, 706, 339, 102, 35, 11], [4750, 2756, 1497, 829, 389, 177, 51, 21], [2842, 1942, 2126, 1143, 859, 373, 139, 52], [666, 1028, 1227, 1240, 869, 592, 177, 114], [504, 623, 1219, 1140, 1053, 815, 285, 169], [112, 317, 667, 1005, 1050, 1783, 415, 606], [49, 145, 323, 558, 1038, 1164, 571, 577], [12, 72, 152, 684, 1063, 11509, 519, 7200]]], "Internal outlinks": [[[87279, 5425, 2456, 595, 283, 79, 36, 10], [5035, 2632, 1344, 713, 380, 158, 49, 23], [2491, 1354, 1735, 1007, 784, 393, 147, 62], [585, 637, 958, 996, 763, 574, 257, 171], [308, 300, 810, 821, 1019, 742, 481, 297], [83, 150, 349, 546, 759, 1092, 603, 1283], [17, 50, 118, 250, 420, 739, 808, 919], [5, 24, 66, 169, 272, 1221, 690, 11537]], [[86557, 5471, 2697, 658, 303, 92, 24, 1], [5149, 2708, 1438, 662, 375, 167, 54, 19], [2458, 1390, 1770, 980, 738, 319, 122, 59], [621, 707, 987, 964, 862, 535, 259, 162], [283, 376, 770, 783, 844, 799, 537, 288], [66, 183, 333, 529, 749, 1156, 737, 1245], [41, 58, 177, 222, 385, 595, 811, 782], [5, 18, 46, 170, 267, 1209, 837, 11750]], [[86929, 4872, 2412, 471, 356, 100, 35, 5], [5697, 2602, 1358, 670, 343, 170, 53, 18], [2875, 1376, 1770, 907, 721, 375, 134, 60], [613, 757, 937, 915, 760, 554, 262, 170], [303, 377, 765, 770, 911, 726, 371, 300], [112, 182, 320, 541, 702, 1158, 649, 1208], [78, 67, 147, 285, 470, 578, 835, 921], [8, 26, 58, 179, 264, 1138, 900, 11733]], [[87744, 5245, 2686, 494, 292, 76, 59, 19], [4855, 2750, 1305, 762, 379, 135, 48, 25], [2512, 1446, 1731, 921, 674, 301, 131, 51], [554, 718, 914, 913, 719, 533, 224, 163], [351, 348, 825, 818, 899, 631, 363, 292], [73, 167, 419, 681, 720, 1036, 575, 1128], [35, 56, 133, 252, 457, 659, 895, 752], [4, 19, 48, 158, 249, 1151, 850, 11936]], [[88472, 4396, 2434, 418, 296, 74, 32, 6], [5740, 2671, 1232, 604, 296, 140, 46, 20], [2872, 1385, 1696, 930, 677, 304, 151, 46], [633, 813, 962, 1041, 710, 460, 222, 158], [356, 380, 764, 717, 777, 672, 480, 243], [96, 181, 325, 539, 663, 1041, 597, 1080], [20, 49, 140, 251, 399, 657, 855, 774], [5, 22, 41, 171, 276, 1027, 830, 11994]], [[90076, 4601, 2555, 495, 353, 71, 37, 6], [5074, 2404, 1272, 595, 301, 178, 49, 24], [2591, 1388, 1578, 894, 702, 284, 112, 45], [554, 753, 862, 888, 751, 484, 227, 152], [365, 376, 676, 718, 748, 632, 364, 215], [87, 143, 359, 515, 655, 987, 600, 1029], [21, 54, 142, 248, 474, 650, 781, 843], [8, 20, 53, 158, 213, 982, 902, 11985]], [[91175, 4189, 2413, 496, 337, 112, 45, 9], [4733, 2471, 1332, 717, 273, 138, 48, 27], [2580, 1192, 1739, 827, 651, 284, 126, 98], [579, 646, 844, 885, 690, 529, 188, 150], [359, 313, 759, 755, 759, 632, 365, 255], [99, 165, 306, 457, 588, 947, 624, 1082], [56, 57, 128, 176, 315, 588, 781, 971], [2, 18, 48, 132, 170, 899, 748, 12282]], [[89027, 3005, 4734, 2123, 512, 154, 27, 1], [3922, 1424, 1694, 1271, 506, 189, 42, 3], [2408, 756, 1828, 1285, 829, 332, 121, 10], [431, 446, 843, 1077, 877, 501, 251, 19], [264, 206, 630, 821, 844, 644, 337, 37], [73, 132, 315, 531, 825, 1039, 1092, 122], [47, 49, 117, 255, 482, 802, 1018, 155], [6, 42, 92, 319, 869, 786, 10632, 2128]], [[91584, 2870, 1239, 254, 162, 50, 17, 2], [3526, 1382, 660, 266, 138, 63, 19, 6], [4091, 3585, 1531, 481, 359, 137, 64, 5], [2830, 2146, 1433, 478, 367, 274, 113, 41], [1065, 1449, 1852, 389, 396, 290, 253, 50], [322, 1042, 1775, 230, 274, 351, 286, 167], [165, 1073, 9722, 107, 240, 298, 1755, 160], [2, 7, 9, 13, 42, 127, 163, 2112]], [[91737, 5892, 3539, 1032, 809, 319, 131, 126], [4295, 2266, 2055, 1502, 1130, 916, 577, 813], [1310, 835, 1381, 1048, 1119, 1616, 1376, 9536], [218, 313, 438, 456, 351, 254, 124, 64], [135, 115, 322, 321, 378, 315, 193, 199], [35, 87, 133, 196, 269, 329, 268, 273], [12, 22, 51, 95, 134, 274, 286, 1796], [1, 6, 12, 26, 51, 147, 179, 2121]], [[88576, 5726, 2505, 572, 251, 72, 33, 8], [4431, 2605, 1287, 707, 309, 152, 35, 10], [2645, 1600, 1769, 917, 631, 256, 86, 27], [530, 846, 1014, 921, 704, 456, 138, 67], [374, 510, 836, 894, 781, 551, 182, 113], [79, 215, 448, 767, 781, 1260, 275, 345], [35, 97, 217, 405, 789, 872, 317, 402], [10, 53, 120, 476, 804, 8750, 344, 4371]]], "External outlinks": [[[118253, 3430, 1938, 280, 180, 68, 13, 15], [3240, 1107, 643, 342, 139, 81, 18, 5], [1931, 610, 965, 504, 390, 173, 66, 18], [236, 362, 401, 510, 349, 243, 97, 64], [188, 159, 357, 368, 350, 268, 168, 124], [28, 102, 141, 213, 272, 404, 201, 440], [18, 121, 65, 110, 180, 210, 330, 326], [3, 23, 19, 91, 111, 423, 290, 3585]], [[117279, 3651, 2358, 351, 194, 47, 14, 3], [3305, 1151, 691, 344, 175, 208, 34, 6], [1964, 538, 997, 443, 361, 154, 56, 16], [267, 338, 478, 506, 378, 279, 93, 79], [181, 141, 339, 348, 389, 293, 183, 97], [34, 69, 194, 225, 276, 380, 211, 481], [12, 24, 72, 103, 145, 176, 362, 289], [2, 10, 72, 69, 77, 387, 346, 3614]], [[117546, 3034, 1961, 249, 199, 38, 12, 5], [3667, 1202, 497, 261, 163, 107, 18, 7], [2409, 636, 1117, 456, 324, 157, 56, 46], [283, 383, 490, 399, 397, 254, 91, 92], [209, 155, 386, 344, 426, 222, 134, 119], [33, 83, 157, 249, 270, 501, 206, 425], [15, 23, 45, 79, 165, 205, 355, 412], [2, 4, 18, 71, 72, 416, 315, 3687]], [[118222, 3507, 1912, 272, 183, 43, 21, 4], [3273, 1122, 539, 337, 166, 58, 15, 10], [2184, 538, 957, 399, 360, 143, 72, 18], [250, 342, 370, 408, 345, 213, 86, 94], [203, 155, 347, 359, 394, 284, 174, 100], [85, 66, 125, 240, 287, 359, 296, 442], [14, 21, 49, 81, 161, 207, 377, 277], [2, 7, 20, 40, 85, 384, 299, 3956]], [[118751, 3023, 1962, 231, 206, 39, 19, 2], [3526, 1183, 556, 242, 149, 59, 27, 16], [1960, 531, 948, 317, 341, 151, 52, 19], [271, 390, 400, 411, 284, 249, 78, 53], [201, 191, 363, 331, 395, 262, 135, 103], [82, 75, 162, 207, 258, 366, 180, 361], [28, 20, 64, 92, 301, 232, 322, 281], [1, 5, 30, 65, 77, 362, 303, 4058]], [[119384, 2918, 2003, 259, 189, 49, 15, 3], [3226, 1109, 516, 303, 163, 83, 13, 5], [1882, 551, 1006, 400, 363, 193, 68, 22], [219, 248, 373, 360, 336, 209, 78, 73], [181, 134, 332, 305, 400, 292, 182, 185], [73, 73, 132, 210, 292, 364, 206, 370], [20, 18, 46, 103, 140, 258, 291, 240], [5, 14, 14, 54, 78, 316, 305, 4107]], [[119703, 2709, 2086, 207, 207, 50, 27, 1], [2945, 1094, 466, 288, 177, 70, 18, 7], [1994, 556, 964, 379, 303, 150, 56, 20], [236, 317, 348, 405, 336, 205, 91, 56], [223, 186, 357, 333, 359, 273, 144, 86], [77, 87, 125, 245, 221, 313, 219, 477], [72, 18, 46, 84, 135, 178, 302, 323], [72, 8, 26, 50, 83, 231, 308, 4227]], [[117600, 2161, 3981, 1235, 253, 79, 13, 0], [2482, 769, 983, 424, 220, 74, 22, 1], [1937, 505, 908, 431, 458, 130, 43, 6], [244, 215, 416, 447, 366, 186, 101, 16], [205, 140, 312, 361, 381, 253, 152, 17], [39, 74, 150, 242, 305, 267, 324, 69], [14, 19, 79, 95, 194, 255, 422, 87], [3, 10, 32, 103, 280, 315, 2963, 1491]], [[119593, 1916, 796, 113, 75, 19, 11, 1], [2686, 721, 254, 121, 71, 22, 14, 4], [3572, 2133, 709, 196, 158, 60, 27, 6], [1455, 755, 584, 195, 171, 96, 63, 19], [523, 610, 693, 176, 180, 129, 124, 22], [137, 345, 527, 89, 119, 132, 120, 90], [45, 312, 2683, 47, 107, 99, 671, 76], [1, 2, 6, 8, 20, 103, 84, 1463]], [[120981, 3375, 2393, 577, 415, 170, 71, 30], [3071, 900, 879, 614, 549, 366, 179, 236], [839, 340, 580, 404, 532, 679, 504, 2374], [101, 122, 184, 178, 172, 114, 43, 31], [80, 49, 146, 166, 176, 121, 85, 78], [18, 21, 48, 77, 107, 155, 98, 136], [6, 8, 38, 42, 59, 130, 125, 706], [1, 7, 5, 12, 17, 94, 62, 1483]], [[119166, 3533, 1909, 307, 142, 24, 9, 7], [2759, 1093, 531, 244, 131, 44, 17, 3], [1900, 662, 930, 386, 252, 98, 34, 11], [322, 372, 460, 413, 296, 134, 43, 30], [257, 294, 436, 356, 356, 195, 92, 41], [50, 138, 219, 308, 374, 445, 131, 164], [27, 49, 96, 190, 240, 288, 123, 154], [7, 14, 61, 132, 225, 2194, 161, 2280]]]}
cpd = {"Page text": [35517, 36536, 41080, 40607, 40406, 40404, 39722, 35830, 37177, 41503, 40821, 40718, 41221, 40241, 35620, 36868, 40867, 41536, 42151, 42596, 40346, 36008, 37019, 40632, 41235, 40417, 40749, 39970, 35944, 36229, 40876, 41319, 40701, 40450, 39061, 35139, 35988, 40336, 40121, 39292, 39515, 38382, 34710, 35373, 39088, 39579, 38491, 39016, 38727, 36114, 36584, 39631, 39805, 39257, 39324, 38812, 35614, 36400, 51365, 49689, 38479, 38961, 38167, 9029, 9127, 40715, 16021, 16093, 40210, 15834, 14388, 36813, 40336, 40239, 40167, 40583, 40240, 36817, 38237, 18979, 19395, 41690, 42141, 40719, 36754, 38284, 42698, 41940, 41108], "Internal outlinks": [25718, 26050, 30383, 30795, 29985, 30026, 29500, 25615, 26525, 30684, 30532, 30369, 30427, 29786, 25462, 26533, 30024, 30649, 31326, 31232, 29560, 25807, 26434, 30327, 30399, 30023, 30313, 29296, 25657, 25959, 30309, 30849, 30429, 29853, 28733, 25334, 25799, 30038, 29626, 29101, 29399, 28131, 24780, 25506, 28901, 29144, 28600, 28670, 28188, 25910, 26010, 29388, 28790, 28669, 28840, 27886, 25386, 26126, 39547, 37849, 28270, 28432, 27507, 5537, 5685, 29897, 10757, 10751, 29384, 10613, 9246, 26417, 29466, 29599, 29618, 29940, 29253, 26247, 27702, 13045, 13462, 30973, 31147, 30170, 26428, 27536, 31526, 31250, 30774], "External outlinks": [10077, 10570, 11804, 11673, 11478, 11768, 11373, 9917, 10560, 11733, 11739, 11745, 11518, 11351, 9951, 10732, 11899, 12017, 12282, 12196, 11735, 10119, 10663, 11483, 11946, 11776, 12064, 11437, 10054, 10559, 11780, 12110, 11762, 11849, 11353, 10117, 10353, 11671, 11533, 11479, 11594, 11045, 9992, 10308, 11450, 11754, 11215, 11692, 11496, 10623, 10391, 11385, 11791, 11306, 11642, 11014, 10258, 10724, 17488, 16461, 11076, 11278, 11021, 3253, 3209, 12189, 5280, 5221, 11584, 5232, 4773, 10762, 11849, 11579, 11521, 11766, 11354, 10520, 10818, 6066, 6248, 12081, 12444, 11866, 10588, 10935, 12350, 12412, 12088]}
cm0 = {"Page text": [[29025, 3730, 3034, 1983, 1850, 1731, 1218, 4220], [28707, 3875, 3044, 2055, 1900, 1830, 1132, 4248], [28356, 3999, 3234, 2059, 1894, 1771, 1208, 4270], [28883, 3814, 3179, 1957, 1805, 1737, 1169, 4247], [29202, 3670, 3045, 1995, 1817, 1605, 1123, 4334], [29695, 3435, 3058, 1845, 1693, 1575, 1138, 4352], [29699, 3551, 2947, 1850, 1699, 1521, 1075, 4449], [29933, 3419, 2988, 1776, 1615, 1417, 1110, 4533], [30217, 3143, 2875, 1825, 1684, 1444, 1160, 4443], [29304, 3543, 3083, 1885, 1760, 1571, 1164, 4481], [28987, 3628, 3182, 1859, 1779, 1514, 1252, 4590], [28215, 3780, 3229, 2074, 1877, 1616, 1289, 4711]], "Internal outlinks": [[33789, 3391, 2529, 1505, 1352, 1150, 716, 2359], [33708, 3527, 2464, 1615, 1312, 1096, 673, 2396], [33392, 3655, 2625, 1565, 1323, 1177, 698, 2356], [33885, 3555, 2483, 1445, 1291, 1067, 662, 2403], [33835, 3531, 2581, 1517, 1241, 949, 679, 2458], [34510, 3201, 2429, 1411, 1147, 975, 676, 2442], [34665, 3217, 2343, 1324, 1186, 933, 663, 2460], [34991, 3051, 2326, 1277, 1067, 901, 642, 2536], [35067, 2930, 2249, 1346, 1101, 928, 695, 2475], [34343, 3143, 2482, 1391, 1209, 1001, 679, 2543], [34024, 3218, 2597, 1434, 1246, 972, 741, 2559], [33036, 3509, 2777, 1650, 1368, 1056, 786, 2609]], "External outlinks": [[40054, 1768, 1299, 690, 594, 553, 293, 1540], [40060, 1836, 1225, 692, 549, 568, 332, 1529], [39879, 1832, 1357, 708, 619, 530, 318, 1548], [40147, 1708, 1330, 667, 541, 506, 286, 1606], [40201, 1713, 1231, 640, 587, 465, 311, 1643], [40364, 1593, 1277, 587, 547, 454, 309, 1660], [40484, 1480, 1212, 612, 603, 449, 272, 1679], [40566, 1475, 1164, 640, 528, 420, 285, 1713], [40580, 1458, 1201, 617, 550, 400, 298, 1687], [40372, 1579, 1218, 610, 566, 440, 325, 1681], [40234, 1578, 1290, 585, 608, 464, 301, 1731], [39893, 1758, 1380, 672, 605, 431, 348, 1704]]}
cc0 = {"Page text": [[[25851, 1859, 893, 215, 133, 50, 20, 4], [1648, 974, 553, 289, 175, 69, 12, 10], [839, 534, 710, 406, 337, 135, 44, 29], [179, 266, 389, 418, 318, 237, 104, 72], [132, 138, 298, 336, 402, 288, 150, 106], [44, 77, 119, 215, 265, 424, 218, 369], [10, 18, 58, 104, 191, 275, 297, 265], [4, 9, 24, 72, 79, 352, 287, 3393]], [[25329, 1924, 1001, 257, 130, 44, 19, 3], [1738, 987, 556, 303, 161, 95, 26, 9], [882, 541, 712, 369, 329, 125, 60, 26], [241, 291, 378, 413, 349, 237, 81, 65], [112, 146, 349, 324, 385, 316, 158, 110], [32, 71, 158, 245, 287, 413, 257, 367], [17, 28, 54, 94, 144, 230, 290, 275], [5, 11, 26, 54, 109, 311, 317, 3415]], [[25494, 1665, 830, 179, 130, 42, 14, 2], [1937, 992, 550, 293, 133, 56, 26, 12], [1018, 548, 740, 380, 301, 156, 61, 30], [250, 316, 439, 391, 298, 195, 97, 73], [122, 172, 333, 297, 394, 294, 175, 107], [46, 80, 191, 229, 299, 377, 229, 320], [14, 25, 65, 117, 158, 251, 302, 276], [2, 16, 31, 71, 92, 366, 265, 3427]], [[25970, 1627, 861, 229, 135, 40, 18, 3], [1839, 933, 526, 278, 140, 63, 23, 12], [957, 547, 731, 393, 332, 134, 53, 32], [238, 296, 391, 368, 303, 196, 110, 55], [140, 164, 309, 336, 349, 285, 141, 81], [38, 62, 149, 230, 291, 353, 250, 364], [17, 23, 59, 113, 161, 223, 279, 294], [3, 18, 19, 48, 106, 311, 249, 3493]], [[26387, 1511, 916, 193, 139, 36, 18, 2], [1865, 860, 472, 250, 126, 70, 21, 6], [989, 487, 710, 341, 281, 141, 64, 32], [238, 304, 426, 393, 279, 204, 97, 54], [151, 159, 320, 312, 364, 262, 150, 99], [48, 82, 135, 221, 258, 330, 215, 316], [12, 24, 61, 96, 146, 242, 269, 273], [5, 8, 18, 39, 100, 290, 304, 3570]], [[26640, 1733, 904, 204, 151, 36, 23, 4], [1629, 824, 480, 238, 157, 80, 16, 11], [991, 483, 735, 373, 264, 139, 45, 28], [226, 265, 352, 379, 298, 171, 113, 41], [151, 148, 281, 282, 336, 248, 161, 86], [43, 71, 106, 221, 272, 345, 207, 310], [17, 17, 62, 97, 155, 252, 258, 280], [2, 10, 27, 56, 66, 250, 252, 3689]], [[26801, 1628, 871, 193, 152, 32, 19, 3], [1778, 852, 465, 240, 128, 53, 26, 9], [975, 455, 734, 339, 252, 121, 52, 19], [218, 232, 377, 363, 287, 211, 104, 58], [115, 137, 316, 308, 354, 231, 147, 91], [34, 76, 144, 187, 241, 311, 207, 321], [10, 31, 57, 101, 133, 201, 271, 271], [2, 8, 24, 45, 68, 257, 284, 3761]], [[27192, 1434, 919, 197, 126, 43, 20, 2], [1696, 806, 431, 248, 145, 64, 25, 4], [953, 452, 707, 357, 293, 138, 67, 21], [191, 233, 319, 391, 297, 194, 108, 43], [131, 126, 287, 298, 333, 229, 130, 81], [31, 61, 135, 190, 249, 305, 212, 234], [16, 18, 56, 84, 163, 209, 275, 289], [7, 13, 21, 60, 78, 262, 323, 3769]], [[26867, 1820, 1058, 235, 161, 46, 28, 2], [1297, 819, 494, 277, 148, 75, 24, 9], [821, 440, 700, 378, 312, 149, 50, 25], [159, 245, 338, 387, 320, 223, 109, 44], [112, 119, 274, 288, 371, 268, 159, 93], [27, 59, 134, 198, 239, 318, 196, 273], [18, 29, 62, 85, 146, 218, 299, 303], [3, 12, 23, 37, 63, 274, 299, 3732]], [[26020, 1868, 997, 201, 157, 42, 16, 3], [1662, 839, 507, 279, 145, 67, 31, 13], [956, 472, 724, 329, 332, 163, 75, 32], [167, 235, 394, 397, 316, 221, 103, 52], [128, 103, 312, 329, 353, 267, 181, 87], [35, 77, 150, 196, 244, 308, 254, 307], [16, 24, 67, 83, 150, 199, 306, 319], [3, 10, 31, 45, 82, 247, 286, 3777]], [[25442, 1958, 1058, 304, 149, 56, 15, 5], [1536, 884, 608, 313, 169, 81, 27, 10], [891, 492, 726, 410, 367, 188, 82, 26], [174, 229, 345, 402, 342, 208, 101, 58], [134, 123, 285, 323, 338, 311, 184, 81], [25, 62, 132, 190, 258, 313, 241, 293], [10, 20, 56, 93, 179, 222, 329, 343], [3, 12, 19, 39, 75, 237, 310, 3895]]], "Internal outlinks": [[[31041, 1690, 746, 179, 90, 30, 10, 3], [1597, 903, 472, 247, 117, 42, 10, 3], [746, 503, 549, 325, 244, 107, 43, 12], [185, 234, 283, 327, 236, 143, 65, 32], [105, 117, 261, 258, 269, 184, 106, 52], [26, 55, 97, 169, 200, 245, 143, 215], [5, 19, 37, 73, 107, 140, 159, 176], [3, 6, 19, 37, 49, 205, 137, 1903]], [[30699, 1805, 851, 201, 105, 38, 8, 1], [1625, 944, 488, 236, 150, 58, 20, 6], [766, 458, 569, 311, 218, 89, 37, 16], [183, 247, 327, 316, 272, 166, 68, 36], [83, 136, 225, 268, 255, 201, 90, 54], [24, 53, 110, 144, 181, 254, 139, 191], [10, 11, 42, 54, 89, 152, 173, 142], [2, 1, 13, 35, 53, 219, 163, 1910]], [[30763, 1642, 695, 149, 106, 23, 13, 1], [1902, 903, 452, 242, 96, 46, 10, 4], [870, 525, 569, 262, 234, 111, 38, 16], [209, 250, 346, 292, 222, 139, 69, 38], [100, 153, 239, 243, 261, 175, 93, 59], [30, 54, 116, 155, 219, 260, 139, 204], [8, 24, 50, 62, 110, 129, 151, 164], [3, 4, 16, 40, 43, 184, 149, 1917]], [[31000, 1719, 837, 180, 105, 25, 19, 0], [1744, 923, 471, 239, 117, 35, 16, 10], [767, 457, 587, 315, 210, 90, 43, 14], [180, 244, 296, 281, 201, 141, 64, 38], [108, 113, 248, 253, 273, 146, 91, 59], [20, 45, 101, 155, 202, 214, 145, 185], [15, 21, 31, 69, 93, 145, 148, 140], [1, 9, 10, 25, 40, 153, 153, 2012]], [[31311, 1448, 782, 157, 106, 25, 5, 1], [1915, 847, 402, 203, 99, 42, 16, 7], [919, 472, 545, 293, 209, 103, 29, 11], [208, 248, 321, 305, 208, 136, 64, 27], [119, 112, 234, 219, 232, 173, 103, 49], [30, 52, 97, 142, 133, 209, 119, 167], [8, 16, 38, 63, 105, 136, 170, 143], [0, 6, 10, 29, 55, 151, 170, 2037]], [[31818, 1596, 785, 160, 109, 27, 14, 1], [1621, 796, 421, 200, 95, 49, 12, 7], [885, 428, 503, 269, 212, 86, 36, 10], [192, 224, 271, 272, 242, 115, 67, 28], [111, 104, 219, 205, 207, 167, 91, 43], [29, 43, 89, 145, 177, 207, 135, 150], [7, 18, 42, 50, 108, 142, 154, 155], [2, 8, 13, 23, 36, 140, 154, 2066]], [[32193, 1446, 740, 146, 99, 31, 9, 1], [1668, 804, 404, 193, 83, 44, 15, 6], [813, 404, 547, 259, 184, 88, 35, 13], [180, 215, 248, 267, 186, 144, 54, 30], [101, 99, 239, 215, 237, 148, 92, 55], [25, 54, 93, 113, 152, 194, 134, 168], [11, 25, 41, 59, 86, 111, 166, 164], [0, 4, 14, 25, 40, 141, 137, 2099]], [[32475, 1461, 769, 157, 94, 28, 6, 1], [1515, 778, 391, 188, 104, 57, 15, 3], [815, 356, 527, 286, 205, 84, 43, 10], [139, 191, 240, 287, 189, 145, 67, 19], [86, 75, 189, 221, 222, 152, 85, 37], [18, 48, 89, 126, 151, 202, 145, 122], [17, 16, 30, 45, 91, 133, 155, 155], [2, 5, 14, 36, 45, 127, 179, 2128]], [[32149, 1653, 871, 209, 126, 45, 12, 2], [1293, 798, 446, 224, 101, 51, 11, 6], [674, 358, 556, 271, 250, 94, 41, 5], [103, 175, 279, 279, 220, 186, 63, 41], [91, 95, 180, 215, 218, 158, 94, 50], [22, 39, 99, 131, 155, 199, 116, 167], [9, 18, 42, 49, 97, 141, 179, 160], [2, 7, 9, 13, 42, 127, 163, 2112]], [[31499, 1687, 833, 173, 107, 29, 10, 5], [1485, 766, 465, 232, 117, 51, 18, 9], [773, 404, 632, 292, 221, 94, 56, 10], [129, 204, 280, 298, 227, 144, 72, 37], [99, 76, 233, 216, 252, 191, 95, 47], [27, 62, 105, 140, 185, 199, 143, 140], [11, 13, 37, 57, 86, 117, 168, 190], [1, 6, 12, 26, 51, 147, 179, 2121]], [[30673, 1884, 1056, 253, 103, 38, 13, 4], [1355, 846, 500, 278, 143, 71, 17, 8], [764, 395, 620, 345, 295, 111, 55, 12], [140, 227, 277, 290, 246, 147, 76, 31], [81, 91, 205, 266, 248, 199, 101, 55], [11, 47, 71, 133, 178, 228, 160, 144], [9, 15, 37, 56, 112, 134, 185, 193], [3, 4, 11, 29, 43, 128, 179, 2162]]], "External outlinks": [[[38388, 1039, 455, 73, 59, 23, 6, 11], [996, 409, 185, 101, 47, 26, 3, 1], [526, 192, 267, 131, 100, 52, 26, 5], [75, 104, 138, 150, 97, 76, 31, 19], [65, 47, 126, 113, 92, 69, 51, 31], [7, 24, 32, 62, 93, 130, 52, 153], [1, 15, 17, 29, 36, 53, 70, 72], [2, 6, 5, 33, 25, 139, 93, 1237]], [[38216, 1087, 578, 94, 61, 20, 4, 0], [1024, 405, 198, 110, 53, 35, 8, 3], [484, 175, 251, 134, 108, 51, 16, 6], [90, 84, 123, 149, 122, 80, 18, 26], [44, 41, 101, 100, 108, 77, 49, 29], [17, 28, 60, 69, 95, 108, 53, 138], [3, 9, 26, 31, 50, 48, 79, 86], [1, 3, 20, 21, 22, 111, 91, 1260]], [[38317, 916, 506, 76, 48, 13, 3, 0], [1108, 387, 171, 91, 46, 24, 2, 3], [575, 182, 312, 129, 96, 43, 16, 4], [87, 123, 140, 135, 104, 72, 23, 24], [41, 60, 138, 117, 117, 65, 43, 38], [13, 34, 44, 71, 71, 118, 41, 138], [4, 5, 15, 23, 38, 51, 84, 98], [2, 1, 4, 25, 21, 120, 74, 1301]], [[38525, 996, 474, 79, 51, 11, 9, 2], [998, 358, 176, 101, 51, 19, 5, 0], [530, 192, 273, 131, 126, 44, 29, 5], [84, 102, 133, 143, 103, 58, 21, 23], [45, 38, 112, 87, 119, 81, 37, 22], [16, 16, 41, 73, 77, 89, 66, 128], [3, 6, 13, 18, 36, 54, 79, 77], [0, 5, 9, 8, 24, 109, 65, 1386]], [[38632, 867, 554, 70, 53, 21, 4, 0], [1044, 375, 137, 92, 38, 17, 7, 3], [521, 166, 271, 99, 103, 51, 15, 5], [85, 108, 131, 120, 94, 63, 25, 14], [64, 48, 112, 100, 121, 69, 37, 36], [15, 25, 42, 62, 73, 88, 64, 96], [3, 4, 24, 26, 49, 57, 78, 70], [0, 0, 6, 18, 16, 88, 79, 1436]], [[38875, 856, 478, 79, 54, 15, 5, 2], [949, 305, 172, 93, 44, 23, 5, 2], [514, 166, 288, 119, 124, 42, 20, 4], [74, 81, 120, 116, 107, 48, 20, 21], [47, 41, 96, 97, 131, 68, 39, 28], [17, 18, 39, 62, 85, 93, 46, 94], [6, 8, 17, 32, 40, 65, 60, 81], [2, 5, 2, 14, 18, 95, 77, 1447]], [[39046, 807, 473, 80, 55, 18, 5, 0], [862, 330, 136, 89, 41, 19, 2, 1], [504, 150, 284, 123, 90, 42, 14, 5], [67, 102, 95, 129, 98, 74, 31, 16], [61, 55, 115, 112, 122, 68, 46, 24], [19, 22, 38, 59, 59, 89, 52, 111], [4, 7, 16, 35, 37, 40, 55, 78], [3, 2, 7, 13, 26, 70, 80, 1478]], [[39102, 840, 495, 59, 54, 10, 6, 0], [825, 312, 174, 89, 48, 17, 9, 1], [493, 149, 247, 121, 99, 33, 16, 6], [88, 79, 119, 147, 99, 63, 29, 16], [53, 41, 98, 93, 116, 69, 41, 17], [14, 28, 45, 69, 76, 62, 57, 69], [4, 5, 17, 18, 35, 58, 61, 87], [1, 4, 6, 21, 23, 88, 79, 1491]], [[38968, 937, 518, 83, 51, 14, 8, 1], [799, 324, 146, 104, 53, 17, 11, 4], [481, 158, 280, 109, 107, 42, 18, 6], [63, 83, 120, 114, 114, 62, 42, 19], [47, 49, 97, 106, 107, 79, 43, 22], [7, 19, 30, 58, 68, 76, 52, 90], [6, 7, 21, 28, 46, 47, 67, 76], [1, 2, 6, 8, 20, 103, 84, 1463]], [[38714, 966, 534, 68, 62, 18, 9, 1], [895, 329, 177, 76, 62, 25, 5, 10], [490, 144, 280, 105, 126, 39, 25, 9], [63, 88, 119, 122, 116, 68, 20, 14], [54, 28, 106, 116, 117, 73, 49, 23], [12, 14, 37, 57, 69, 95, 53, 103], [5, 2, 32, 29, 39, 52, 78, 88], [1, 7, 5, 12, 17, 94, 62, 1483]], [[38390, 1049, 612, 101, 60, 13, 5, 4], [874, 355, 170, 98, 51, 18, 10, 2], [487, 188, 298, 135, 112, 44, 21, 5], [65, 84, 112, 130, 92, 58, 27, 17], [53, 56, 107, 109, 136, 73, 48, 26], [13, 19, 52, 50, 79, 102, 68, 81], [8, 5, 20, 32, 45, 43, 66, 82], [3, 2, 9, 17, 30, 80, 103, 1487]]]}
cpd0 = {"Page text": [9089, 9014, 10104, 10130, 10112, 10156, 10073, 9061, 9138, 10377, 10256, 10162, 10348, 10080, 9045, 9047, 10319, 10290, 10571, 10647, 10163, 9176, 9116, 10150, 10421, 10051, 10029, 9949, 8975, 9063, 10186, 10107, 10029, 10139, 9777, 8813, 8863, 9979, 10071, 9836, 9957, 9604, 8715, 8799, 9840, 9850, 9652, 9704, 9799, 9345, 9087, 9850, 9828, 9727, 9726, 9559, 8882, 8994, 9941, 9707, 9485, 9643, 9586, 9029, 9127, 10141, 10059, 10133, 10095, 9908, 9147, 9215, 10087, 10196, 10230, 10363, 10387, 9419, 9551, 10668, 11034, 10837, 10782, 10423, 9464, 9565, 10880, 10606, 10460], "Internal outlinks": [5948, 5640, 6743, 6860, 6689, 6692, 6636, 5671, 5730, 6814, 6719, 6624, 6715, 6584, 5652, 5659, 6707, 6723, 6999, 7013, 6605, 5751, 5717, 6641, 6667, 6550, 6495, 6480, 5598, 5621, 6618, 6915, 6778, 6535, 6305, 5461, 5452, 6584, 6521, 6356, 6448, 6150, 5394, 5425, 6282, 6270, 6213, 6231, 6243, 5818, 5564, 6363, 6197, 6138, 6234, 5976, 5439, 5619, 6354, 6147, 6102, 6177, 6069, 5537, 5685, 6599, 6542, 6530, 6538, 6405, 5697, 5844, 6568, 6651, 6569, 6727, 6667, 5891, 6021, 7024, 7377, 7307, 7127, 6868, 6020, 6187, 7194, 7064, 6948], "External outlinks": [3212, 3201, 3494, 3498, 3600, 3585, 3556, 3181, 3256, 3547, 3548, 3502, 3575, 3485, 3180, 3212, 3583, 3544, 3661, 3707, 3614, 3219, 3158, 3543, 3637, 3536, 3529, 3456, 3162, 3188, 3615, 3571, 3510, 3563, 3469, 3219, 3139, 3541, 3538, 3537, 3556, 3389, 3140, 3156, 3451, 3509, 3442, 3476, 3474, 3274, 3196, 3493, 3516, 3452, 3431, 3372, 3176, 3178, 3497, 3381, 3391, 3420, 3388, 3253, 3209, 3593, 3515, 3477, 3503, 3504, 3225, 3289, 3581, 3587, 3514, 3653, 3641, 3323, 3313, 3660, 3794, 3727, 3718, 3597, 3316, 3265, 3649, 3638, 3598]}
cpd0_sun = {"Page text": [9014, 9138, 9047, 9116, 9063, 8863, 8799, 9087, 8994, 9127, 9215, 9551, 9565], "Internal outlinks": [5640, 5730, 5659, 5717, 5621, 5452, 5425, 5564, 5619, 5685, 5844, 6021, 6187], "External outlinks": [3201, 3256, 3212, 3158, 3188, 3139, 3156, 3196, 3178, 3209, 3289, 3313, 3265]}
cpd0_tue = {"Page text": [10112, 10162, 10571, 10051, 10029, 9836, 9652, 9727, 9485, 10133, 10230, 10837, 10460], "Internal outlinks": [6689, 6624, 6999, 6550, 6778, 6356, 6213, 6138, 6102, 6530, 6569, 7307, 6948], "External outlinks": [3600, 3502, 3661, 3536, 3510, 3537, 3442, 3452, 3391, 3477, 3514, 3727, 3598]}

cm, cc, cpd = normalize_results(cm, cc, cpd)
cm0_backup = cm0
cm0, cc0, cpd0 = normalize_results(cm0, cc0, cpd0)
_, _, cpd0_sun = normalize_results(cm0_backup, cc0, cpd0_sun)
_, _, cpd0_tue = normalize_results(cm0_backup, cc0, cpd0_tue)

# cpp, pcpd = compute_changes_per_page()
# rpc = compute_rare_changes(cpp, pcpd)
# dcf = compute_day_change_fractions(cpp)

# acb = compute_average_change_behaviour(cm0)


# DAILY CHANGES PLOTS
# draw_page_changes_per_day(cpd)
# draw_rare_page_changes(rpc)
# draw_day_change_fractions_percentile(dcf)
# draw_page_changes_per_day(cpd0, plot_multiple=True, corrected_dataset=True)
# draw_page_changes_per_single_day(cpd0_sun, cpd0_tue)

# WEEKLY CHANGES PLOTS ALLUVIAL
# draw_alluvial_plot(cc0["Page text"], "page text")
# draw_alluvial_plot(cc0["Internal outlinks"], "internal out-links")
# draw_alluvial_plot(cc0["External outlinks"], "external out-links")

# WEEKLY CHANGES PLOTS NORMAL
# draw_average_change_behaviour_fractions(acb, "text")
# draw_average_change_behaviour_fractions(acb, "other")
# draw_change_behaviour_per_week(cm0["Page text"])
# draw_same_change_behaviour_fractions(cc0["Page text"])
# draw_same_change_behaviour_fractions_combined(cc0["Page text"])
# draw_neighbouring_change_behaviour_fractions(cc0["Page text"])

# WEEKLY CHANGES PLOTS THREE CLASSES
# draw_same_change_behaviour_fractions(cc0["Page text"], limit_three_classes=True)
# draw_same_change_behaviour_fractions_combined(cc0["Page text"], limit_three_classes=True)
# draw_neighbouring_change_behaviour_fractions(cc0["Page text"], limit_three_classes=True)

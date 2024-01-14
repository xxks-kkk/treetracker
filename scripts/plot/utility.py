from typing import List

import jinja2

from validation_error import ValidationError


def check_argument(predicate, message):
    if predicate:
        return
    else:
        raise ValidationError(message)


class TimeUnits:
    SECONDS = "SECONDS"
    MINUTES = "MINUTES"
    MILISECONDS = "MILISECONDS"


def convert_time(data: List[float], unit: str) -> List[float]:
    """
    Convert the given datapoint in data (ms) to the given unit
    """
    if unit == TimeUnits.SECONDS:
        return [dp / 1000 for dp in data]
    if unit == TimeUnits.MINUTES:
        return [dp / 1000 / 60 for dp in data]
    if unit == TimeUnits.MILISECONDS:
        return data
    raise ValidationError(f"Given unit: {unit} is unsupported")


def find_non_zero_min(data: List[float]) -> float:
    """
    Find the non-zero minimum value in data
    """
    non_zero_min = 999999999
    for dp in data:
        if dp != 0 and dp < non_zero_min:
            non_zero_min = dp
    return non_zero_min


def normalized_data(data: List[float], min_all: float, max_all: float) -> List[float]:
    """
    Normalize the datapoint in data
    """
    return [(dp - min_all) / (max_all - min_all) for dp in data]


def to_float(x: str) -> float:
    """
    Convert x to float. If x cannot be converted, return 0.
    """
    try:
        return float(x)
    except (TypeError, ValueError):
        return 0


FILENAME_SIGNATURE = \
    "{{ file_name_signature }}" \
    "{% if enable_log_scale %}_log{% endif %}" \
    "{% if focus_dp_with_cost_diff_greater_than_zero %}_focus_dp_with_cost_diff_greater_than_zero{% endif %}" \
    "{% if use_full_reducer_lower_bound %}_use_full_reducer_lower_bound{% endif %}" \
    "{% if times_num_relations_to_output_size %}_times_num_relations_to_output_size{% endif %}" \
    "{% if enable_linear_regression %}_enable_linear_regression{% endif %}" \
    ".pdf"


def render_filename(file_name_signature: str,
                    enable_log_scale=False,
                    focus_dp_with_cost_diff_greater_than_zero=False,
                    use_full_reducer_lower_bound=False,
                    times_num_relations_to_output_size=False,
                    enable_linear_regression=False):
    arguments = locals()
    parameters = dict(locals())
    return jinja2.Template(FILENAME_SIGNATURE).render(parameters)

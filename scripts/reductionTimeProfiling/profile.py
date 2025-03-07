"""
3. Perform profiling on the engine and generate profile result
4. Parse the profile result and produce statistics related to reduction time percentage
"""
import re
import subprocess
from pathlib import Path

import jinja2
from bs4 import BeautifulSoup
from jinja2 import FileSystemLoader

from reductionTimeProfiling.proggen import Benchmark, get_template_dir, Algorithm, SSBQuery, \
    get_reduction_time_profiling_class, get_mvn_root_dir, TTJHPJOB, Yannakakis1PassJOB, get_algorithm_name, \
    get_project_dir


class ProfileResult:
    def __init__(self,
                 total_sample_count,
                 main_sample_count,
                 join_fragment_eval_count,
                 cursor_count):
        self.total_sample_count = total_sample_count
        self.main_sample_count = main_sample_count
        self.join_fragment_eval_count = join_fragment_eval_count
        self.cursor_count = cursor_count

class  TTJProfileResult(ProfileResult):
    def __init__(self,
                 total_sample_count,
                 main_sample_count,
                 join_fragment_eval_count,
                 cursor_count,
                 ng_probing_count,
                 ng_construct_count,
                 remove_dangling_tuple_from_rinner_count):
        super().__init__(total_sample_count, main_sample_count, join_fragment_eval_count, cursor_count)
        self.ng_probing_count = ng_probing_count
        self.ng_construct_count = ng_construct_count
        #TODO: need to consider `extract()` called in PassContext() as well; not just il.remove()
        self.remove_dangling_tuple_from_rinner_count = remove_dangling_tuple_from_rinner_count

    def __repr__(self):
        return f"""
        TTJProfileResult
        ----------------
        total_sample_count                     : {self.total_sample_count} 
        main_sample_count                      : {self.main_sample_count}
        join_fragment_eval_count               : {self.join_fragment_eval_count}
        cursor_count                           : {self.cursor_count}
        ng_probing_count                       : {self.ng_probing_count}
        ng_construct_count                     : {self.ng_construct_count}
        remove_dangling_tuple_from_rinner_count: {self.remove_dangling_tuple_from_rinner_count}
        """

class LIPProfileResult(ProfileResult):
    def __init__(self,
                total_sample_count,
                main_sample_count,
                join_fragment_eval_count,
                cursor_count,
                bloom_filter_build_count,
                bloom_filter_probe_count):
        super().__init__(total_sample_count, main_sample_count,join_fragment_eval_count, cursor_count)
        self.bloom_filter_build_count = bloom_filter_build_count
        self.bloom_filter_probe_count = bloom_filter_probe_count

    def __repr__(self):
        return f"""
        LIPProfileResult
        ----------------
        total_sample_count                     : {self.total_sample_count} 
        main_sample_count                      : {self.main_sample_count}
        join_fragment_eval_count               : {self.join_fragment_eval_count}
        cursor_count                           : {self.cursor_count}
        bloom_filter_build_count               : {self.bloom_filter_build_count}
        bloom_filter_probe_count               : {self.bloom_filter_probe_count}
        """

class YannakakisProfileResult(ProfileResult):
    def __init__(self,
                total_sample_count,
                main_sample_count,
                join_fragment_eval_count,
                cursor_count,
                full_reducer_count):
        super().__init__(total_sample_count, main_sample_count, join_fragment_eval_count, cursor_count)
        self.full_reducer_count = full_reducer_count

    def __repr__(self):
        return f"""
        YannakakisProfileResult
        ----------------
        total_sample_count                     : {self.total_sample_count} 
        main_sample_count                      : {self.main_sample_count}
        join_fragment_eval_count               : {self.join_fragment_eval_count}
        cursor_count                           : {self.cursor_count}
        full_reducer_count                     : {self.full_reducer_count}
        """

def get_profile_result_dir():
    return get_project_dir() / "results" / "others" / "profiling"


def get_profile_result_file(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    return get_profile_result_dir() / f"{benchmark}_{algorithm}_{query}.html"


def get_profile_script_name(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    return get_mvn_root_dir() / f"ProfileReductionTimeProfiling_{benchmark}_{algorithm}_{query}.sh"


def make_profile_executable(profile_script_name):
    subprocess.call(f"chmod 755 {profile_script_name}", shell=True, cwd=get_mvn_root_dir())


def create_profile_script(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("ProfileReductionTimeProfiling.sht")
    reduction_time_profiling_class = get_reduction_time_profiling_class(benchmark, get_algorithm_name(algorithm))
    content = template.render(benchmark=benchmark,
                              ReductionTimeProfilingClassName=reduction_time_profiling_class,
                              profile_result_file=get_profile_result_file(benchmark, algorithm, query),
                              project_dir=get_project_dir())
    filename = get_profile_script_name(benchmark, algorithm, query)
    with open(filename, mode="w", encoding="utf-8") as profile_script:
        profile_script.write(content)
        print(f"... wrote {filename}")
        make_profile_executable(filename)


def execute_profile_script(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    subprocess.call(f"{get_profile_script_name(benchmark, algorithm, query)}", shell=True, cwd=get_mvn_root_dir())


def construct_profile_result(benchmark: Benchmark, algorithm: Algorithm, query: SSBQuery):
    profile_result_file = get_profile_result_file(benchmark, algorithm, query)
    print(f"processing {profile_result_file} ...")

    def extract_sample_count(text_in_div):
        """
        Expect '46.96% [1,119] • self: 0.00% [0]', which is wrapped in <div> tag of the profiling result.
        Then, extract '1119' from it and return.
        """
        return int(re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", text_in_div.split(' ')[1])[0].replace(',',''))

    def extract_total_samples(text_in_div):
        """
        Expect 'Call tree view, total samples: 749 ' and return '749'
        """
        return int(re.findall("[-+]?[.]?[\d]+(?:,\d\d\d)*[\.]?\d*(?:[eE][-+]?\d+)?", text_in_div)[0].replace(',',''))

    def construct_TTJ_profile_result():
        total_sample_count = 0
        main_sample_count = 0
        ng_probing_count = 0
        ng_construct_count = 0
        remove_dangling_tuple_from_rinner_count = 0
        join_fragment_eval_count = 0
        cursor_count = 0
        with open(profile_result_file) as f:
            for line in f:
                if 'isGood' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for ng_probing_count")
                    ng_probing_count += extract_sample_count(soup.find('div').text)
                elif 'total samples' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    total_sample_count = extract_total_samples(soup.find('span').text)
                elif f'{get_reduction_time_profiling_class(benchmark, algorithm)}.main' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for main_sample_count")
                    main_sample_count = extract_sample_count(soup.find('div').text)
                elif 'updateNoGoodListMap' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for ng_construct_count")
                    ng_construct_count += extract_sample_count(soup.find('div').text)
                elif 'JoinFragment.eval' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for JoinFragment.eval")
                    join_fragment_eval_count += extract_sample_count(soup.find('div').text)
                #TODO: we need to implement remove_dangling_tuple_from_rinner_count when producing JOB data
                elif 'JdbcRecordSet.cursor' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for JdbcRecordSet.cursor")
                    cursor_count += extract_sample_count(soup.find('div').text)

        return TTJProfileResult(total_sample_count,
                                main_sample_count,
                                join_fragment_eval_count,
                                cursor_count,
                                ng_probing_count,
                                ng_construct_count,
                                remove_dangling_tuple_from_rinner_count)

    def construct_LIP_profile_result():
        total_sample_count = 0
        main_sample_count = 0
        bloom_filter_build_count = 0
        bloom_filter_probe_count = 0
        join_fragment_eval_count = 0
        cursor_count = 0
        with open(profile_result_file) as f:
            for line in f:
                if 'isGood' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for bloom_filter_probe_count")
                    bloom_filter_probe_count += extract_sample_count(soup.find('div').text)
                elif 'total samples' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    total_sample_count = extract_total_samples(soup.find('div').text)
                elif f'{get_reduction_time_profiling_class(benchmark, algorithm)}.main' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for main_sample_count")
                    main_sample_count = extract_sample_count(soup.find('div').text)
                elif 'constructJavForBloomFilters' in line and 'lambda$constructJavForBloomFilters$0' not in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for bloom_filter_build_count")
                    bloom_filter_build_count += extract_sample_count(soup.find('div').text)
                elif 'JoinFragment.eval' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for JoinFragment.eval")
                    join_fragment_eval_count += extract_sample_count(soup.find('div').text)
                elif 'JdbcRecordSet.cursor' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for JdbcRecordSet.cursor")
                    cursor_count += extract_sample_count(soup.find('div').text)

        return LIPProfileResult(total_sample_count,
                                main_sample_count,
                                join_fragment_eval_count,
                                cursor_count,
                                bloom_filter_build_count,
                                bloom_filter_probe_count)

    def construct_Yannakakis_profile_result():
        total_sample_count = 0
        main_sample_count = 0
        full_reducer_count = 0
        join_fragment_eval_count = 0
        cursor_count = 0
        with open(profile_result_file) as f:
            for line in f:
                if 'FullReducerOperator.executeSemiJoins' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for full_reducer_count")
                    full_reducer_count += extract_sample_count(soup.find('div').text)
                elif 'total samples' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    total_sample_count = extract_total_samples(soup.find('span').text)
                elif f'{get_reduction_time_profiling_class(benchmark, algorithm)}.main' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for main_sample_count")
                    main_sample_count = extract_sample_count(soup.find('div').text)
                elif 'JoinFragment.eval' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for JoinFragment.eval")
                    join_fragment_eval_count += extract_sample_count(soup.find('div').text)
                elif 'JdbcRecordSet.cursor' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for JdbcRecordSet.cursor")
                    cursor_count += extract_sample_count(soup.find('div').text)

        return YannakakisProfileResult(total_sample_count,
                                    main_sample_count,
                                    join_fragment_eval_count,
                                    cursor_count,
                                    full_reducer_count)

    if algorithm == Algorithm.TTJ:
        return construct_TTJ_profile_result()
    elif algorithm == Algorithm.LIP:
        return construct_LIP_profile_result()
    elif algorithm == Algorithm.Yannakakis:
        return construct_Yannakakis_profile_result()
    elif algorithm == Algorithm.YannakakisB:
        return construct_Yannakakis_profile_result()
    elif algorithm == Algorithm.Yannakakis1Pass:
        return construct_Yannakakis_profile_result()

if __name__ == "__main__":
    # construct_profile_result(Benchmark.job, Algorithm.TTJ, TTJHPJOB.Query11bOptJoinTreeOptOrderingShallowHJOrdering)
    construct_profile_result(Benchmark.job, Algorithm.Yannakakis1Pass, Yannakakis1PassJOB.Query11bOptJoinTreeOptOrderingY1PShallowHJOrdering)

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

from exp2p5.proggen import Benchmark, Algorithm, Exp2P5Query, get_mvn_root_dir, get_template_dir, get_exp2p5_class
from plot.utility import check_argument


class ProfileResult:
    def __init__(self,
                 total_sample_count,
                 main_sample_count):
        self.total_sample_count = total_sample_count
        self.main_sample_count = main_sample_count

class  TTJProfileResult(ProfileResult):
    def __init__(self,
                 total_sample_count,
                 main_sample_count,
                 ng_probing_count,
                 ng_construct_count,
                 remove_dangling_tuple_from_rinner_count):
        super().__init__(total_sample_count, main_sample_count)
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
        ng_probing_count                       : {self.ng_probing_count}
        ng_construct_count                     : {self.ng_construct_count}
        remove_dangling_tuple_from_rinner_count: {self.remove_dangling_tuple_from_rinner_count}
        """

def get_profile_result_dir():
    return Path.home() / "projects" / "treetracker2" / "results" / "others" / "exp2p5"


def get_profile_result_file(benchmark: Benchmark, algorithm: Algorithm, query: Exp2P5Query):
    return get_profile_result_dir() / f"{benchmark}_{algorithm}_{query}.html"


def get_profile_script_name(benchmark: Benchmark, algorithm: Algorithm, query: Exp2P5Query):
    return get_mvn_root_dir() / f"exp2p5Profiling_{benchmark}_{algorithm}_{query}.sh"


def make_profile_executable(profile_script_name):
    subprocess.call(f"chmod 755 {profile_script_name}", shell=True, cwd=get_mvn_root_dir())


def create_profile_script(benchmark: Benchmark, algorithm: Algorithm, query: Exp2P5Query):
    environment = jinja2.Environment(loader=FileSystemLoader(get_template_dir()),
                                     keep_trailing_newline=True)
    template = environment.get_template("ProfileExp2P5Profiling.sht")
    reduction_time_profiling_class = get_exp2p5_class()
    content = template.render(benchmark=benchmark,
                              ReductionTimeProfilingClassName=reduction_time_profiling_class,
                              profile_result_file=get_profile_result_file(benchmark, algorithm, query))
    filename = get_profile_script_name(benchmark, algorithm, query)
    with open(filename, mode="w", encoding="utf-8") as profile_script:
        profile_script.write(content)
        print(f"... wrote {filename}")
        make_profile_executable(filename)


def execute_profile_script(benchmark: Benchmark, algorithm: Algorithm, query: Exp2P5Query):
    subprocess.call(f"{get_profile_script_name(benchmark, algorithm, query)}", shell=True, cwd=get_mvn_root_dir())


def construct_profile_result(benchmark: Benchmark, algorithm: Algorithm, query: Exp2P5Query):
    profile_result_file = get_profile_result_file(benchmark, algorithm, query)
    print(f"processing {profile_result_file} ...")

    def extract_sample_count(text_in_div):
        """
        Expect '[8] 0.57% 4 self: 0.14% 1', which is wrapped in <div> tag of the profiling result.
        Then, extract '4' from it and return.
        """
        return int(text_in_div.split(' ')[2].replace(',', ''))

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
        with open(profile_result_file) as f:
            for line in f:
                if 'isGood' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for ng_probing_count")
                    ng_probing_count += extract_sample_count(soup.find('div').text)
                elif 'total samples' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    total_sample_count = extract_total_samples(soup.find('div').text)
                elif f'{get_exp2p5_class()}.main' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for main_sample_count")
                    main_sample_count = extract_sample_count(soup.find('div').text)
                elif 'updateNoGoodListMap' in line:
                    soup = BeautifulSoup(line, "html.parser")
                    print(f"parse: {soup.find('span').text} for ng_construct_count")
                    ng_construct_count += extract_sample_count(soup.find('div').text)
                #TODO: we need to implement remove_dangling_tuple_from_rinner_count when producing JOB data

        return TTJProfileResult(total_sample_count,
                                main_sample_count,
                                ng_probing_count,
                                ng_construct_count,
                                remove_dangling_tuple_from_rinner_count)


    check_argument(algorithm == Algorithm.TTJ, "has to be Algorithm.TTJ")
    construct_result = construct_TTJ_profile_result()
    print(construct_result)
    return construct_result

if __name__ == "__main__":
    construct_profile_result(Benchmark.exp2p5, Algorithm.TTJ, Exp2P5Query.Exp2P5Query0P)

print("Documentation for compare_datasets library is available at: https://compare-dataset-docs.vercel.app/compare_datasets_api/Compare")
from compare_datasets.prepare import PrepareForComparison
from compare_datasets.string_comparisons import StringComparisons
from compare_datasets.numeric_comparisons import NumericComparisons
from compare_datasets.datetime_comparison import DateTimeComparisons
from compare_datasets.boolean_comparison import BooleanComparisons
from compare_datasets.jaccard_similarity import JaccardSimilarity
from compare_datasets.structure import stringify_result
from compare_datasets.html_report import generate_body
from datetime import datetime
import importlib.resources
from tqdm import tqdm
from jinja2 import Template
import tabulate
import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Compare:
    
    def __init__ (self, tested, expected, key=None, verbose=False, low_memory=False, strict_schema=False, case_sensitive_column_names=False, numeric_tolerance=4):
        
        self.verbose = verbose
        self.progress_bar = tqdm(total=100,desc="Preparing datasets", bar_format="{desc}: {percentage:2.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
        self.progress_bar.update(5)
        self.data = PrepareForComparison(tested, expected, key, verbose=verbose, progress_bar=self.progress_bar, low_memory=low_memory, strict_schema=strict_schema, case_sensitive_columns=case_sensitive_column_names, numeric_tolerance=numeric_tolerance)       
        self.result = [self.data.overall_result]
        self.jaccard_similarity = JaccardSimilarity(prepared_data=self.data, verbose=self.data.verbose, progress_bar=self.progress_bar) 
        self.progress_bar.update(10)
        self.tested = self.data.tested
        self.expected = self.data.expected
        self.metadata = self.__metadata__()
        
        if len(self.data.column_list["String Columns"]) != 0:        
            self.string_comparisons = StringComparisons(prepared_data=self.data, verbose=self.data.verbose,progress_bar=self.progress_bar, low_memory=low_memory)
            self.result.append(self.string_comparisons.result)
        
        self.progress_bar.update(20)        

        if len(self.data.column_list["Numeric Columns"]) != 0:
            self.numeric_comparisons = NumericComparisons(prepared_data=self.data, verbose=self.data.verbose, progress_bar=self.progress_bar)
            self.result.append(self.numeric_comparisons.result)
            
        if len(self.data.column_list["Datetime Columns"]) != 0:
            self.date_comparisons = DateTimeComparisons(prepared_data=self.data, verbose=self.data.verbose, progress_bar=self.progress_bar)
            self.result.append(self.date_comparisons.result)    
            
        if len(self.data.column_list["Boolean Columns"]) != 0:
            self.boolean_comparisons = BooleanComparisons(prepared_data=self.data, verbose=self.data.verbose, progress_bar=self.progress_bar)
            self.result.append(self.boolean_comparisons.result)        
            
        self.progress_bar.update(20)
        self.progress_bar.set_description("Comparison Completed Successfully. Please print the object to view the report")
        self.progress_bar.close()
 
        
    def report (self):
        report = []
        report.append("COMPARISON REPORT\n=================")
        report.append(f"OVERALL RESULT: {stringify_result(all(self.result))}")
        report.append(self.data.__str__())
        report.append(self.jaccard_similarity.__str__())
        if len(self.data.column_list["String Columns"]) != 0:
            report.append(self.string_comparisons.__str__())
        if len(self.data.column_list["Numeric Columns"]) != 0:
            report.append(self.numeric_comparisons.__str__())
        if len(self.data.column_list["Datetime Columns"]) != 0:
            report.append(self.date_comparisons.__str__())
        if len(self.data.column_list["Boolean Columns"]) != 0:
            report.append(self.boolean_comparisons.__str__())
        return "\n \n".join(report)
        
    def __repr__ (self):
        return self.report()

      
    def get_report (self, save_at_path=None, filename=None, format='txt', tested_frame_name=None, expected_frame_name=None, conclusion=None):
        
        if not tested_frame_name is None:
            self.metadata['Tested Frame Name'] = tested_frame_name
        if not expected_frame_name is None:
            self.metadata['Expected Frame Name'] = expected_frame_name     
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        if filename is None:
            filename = f"report_{timestamp}.{format}"
        if format not in ['txt', 'html']:
            raise Exception("Invalid format. Please use 'text' or 'html'")
        if format == 'txt':
            report = self.report()   
        if format == 'html':
            data = {'content': generate_body(self), 'analysis':''}
            if self.verbose:
                print(data)
            p = importlib.resources.as_file(importlib.resources.files('compare_datasets.resources') / 'report_template.html')
            with p as f:
                template = Template(f.read_text('utf8'))    # as an example
            report = template.render(data)       
        if not save_at_path is None:
            if save_at_path.endswith("/"):
                save_at_path =  save_at_path[:-1]
            save_at_path = f"{save_at_path}/{filename}"                     
        else:
            save_at_path = f"./{filename}"            
        with open(save_at_path, "w",encoding="utf-8") as f:
                f.write(report)
        return f"Report has been successfully saved at: {save_at_path}"
    
    def __metadata__ (self):
        return {
            "Execution Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Compared on": self.data.key,    
        }
            


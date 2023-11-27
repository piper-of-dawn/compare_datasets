from compare_datasets.prepare import PrepareForComparison
from compare_datasets.string_comparisons import StringComparisons
from compare_datasets.numeric_comparisons import NumericComparisons
from compare_datasets.datetime_comparison import DateTimeComparisons
from compare_datasets.boolean_comparison import BooleanComparisons
from compare_datasets.structure import stringify_result
from datetime import datetime
from tqdm import tqdm
from compare_datasets.structure import calculate_jaccard_similarity, generate_report

class Compare:
    def __init__ (self, tested, expected, key=None, verbose=False):
        self.result = []
        self.progress_bar = tqdm(total=100,desc="Preparing datasets", bar_format="{desc}: {percentage:2.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]")
        self.progress_bar.update(5)
        self.data = PrepareForComparison(tested, expected, key, verbose=verbose, progress_bar=self.progress_bar)       
        self.jaccard_report = calculate_jaccard_similarity(self.data.tested, self.data.expected, self.data.intersection) 
        self.progress_bar.update(10)
        self.tested = self.data.tested
        self.expected = self.data.expected
        
        if len(self.data.column_list["String Columns"]) != 0:        
            self.string_comparisons = StringComparisons(prepared_data=self.data, verbose=self.data.verbose,progress_bar=self.progress_bar)
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
        report.append(generate_report(self.jaccard_report))
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

      
    def save_report (self, path=""):     
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        filename = f"report_{timestamp}.txt"
        report = self.report()   
        with open(f"{path}/{filename}", "w",encoding="utf-8") as f:
            f.write(report)
        


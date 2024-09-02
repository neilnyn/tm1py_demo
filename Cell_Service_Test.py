import pandas as pd
from TM1py.Services import TM1Service
import configparser
import datetime
import logging
import mdxpy

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    filename='cell_service_test.log', filemode='w')


# logging decorator with parameter which set the context of the log message
def log_context(context):
    def log_decorator(func):
        def wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__name__)
            logger.info("Start" + context)
            before = datetime.datetime.now()
            result = func(*args, **kwargs)
            after = datetime.datetime.now()
            elapsed_time = after - before
            logger.info("End" + context)
            logger.info("Elapsed time: " + str(elapsed_time))
            return result

        return wrapper

    return log_decorator


# read config file
config = configparser.ConfigParser()
config.read('config.ini')

# connect to TM1
tm1 = TM1Service(**config['Neil'])

df_load = pd.read_csv('data.csv')


# Test Write Function
@log_context("write_dataframe")
def test_write_dataframe():
    # 40万行数据写入
    # 性能注解 use_ti,use_blob=false,则使用原生的cellset进行数据写入,耗时最久
    # 性能注解 use_ti=True,使用unbound TI进行数据写入，速度第二快
    # 性能注解 use_blob=True,使用blob进行数据写入，速度最快
    # 性能注解 use_ti=True,use_blob=True,则还是使用unbound TI进行数据写入.
    tm1.cells.write_dataframe(cube_name='TM1py_Demo',
                              data=df_load,
                              dimensions=['Date', 'Region', 'Product'],
                              deactivate_transaction_log=True,
                              reactivate_transaction_log=True,
                              use_ti=False,
                              use_blob=True,
                              skip_non_updateable=True)


@log_context("write_dataframe_async")
def test_write_dataframe_async():
    # 性能注解 40万数据耗时最短,异步比传统的要快,默认使用use_blob=True,使用api/vi/Content('Blobs')/Contents('tm1py.xxx.csv')进行写入
    tm1.cells.write_dataframe_async(cube_name='TM1py_Demo',
                                    data=df_load,
                                    dimensions=['Date', 'Region', 'Product'],
                                    deactivate_transaction_log=True,
                                    reactivate_transaction_log=True,
                                    slice_size_of_dataframe=1000,
                                    max_workers=8)


def test_write():
    tm1.cells.write(cube_name='TM1py_Demo',
                    cellset_as_dict={},
                    dimensions=['Date', 'Region', 'Product'],
                    deactivate_transaction_log=True,
                    reactivate_transaction_log=True,
                    use_ti=False,
                    use_blob=True,
                    use_changeset=False,
                    measure_dimension_elements={'Amount': 'Numeric'})


# Test Execute Function
# use mdxpy build mdx
mdx = '''
    SELECT 
          NON EMPTY
          {[Measure Dimension].[Amount]}
          ON COLUMNS,
          NON EMPTY
          {TM1FilterbyLevel(TM1SubsetAll([Scenario]),0)}
          *
          {TM1FilterbyLevel(TM1SubsetAll([Region]),0)}
          *
          {TM1FilterbyLevel(TM1SubsetAll([Product]),0)}
          ON ROWS
          FROM [TM1py_Demo]
      '''


def test_execute_mdx(mdx, skip_contexts=True, skip_cell_properties=True, skip_zeros=True,
                     skip_consolidated_cells=True, skip_rule_derived_cells=True, max_worker=1,
                     element_unique_names=False):
    # 注解 skip_contexts 省略 title字段的内容,skip_cell_properties 省略单元格属性,max_worker开启的线程数,async_axis=0/1在行或列方向开启多线程
    # 性能注解 这里的max_worker 无法显著提升性能
    cellset = tm1.cells.execute_mdx(mdx, skip_contexts=True,
                                    skip_cell_properties=True,
                                    skip_zeros=True,
                                    skip_consolidated_cells=True,
                                    skip_rule_derived_cells=True,
                                    max_worker=1,
                                    element_unique_names=False)
    return cellset

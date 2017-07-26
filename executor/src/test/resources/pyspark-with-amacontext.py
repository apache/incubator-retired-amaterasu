numDS = ama_context.get_dataframe("test_action3", 'numDS')
odd = numDS.filter(numDS['_1'] % 2 != 0)
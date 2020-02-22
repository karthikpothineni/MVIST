import csv
import pandas as pd
from datetime import datetime
from collections import OrderedDict
import os

input_data_path = '/Users/venkatasriramkarthik/Documents/mvist_data/22022020/'
output_data_path = input_data_path+'output/'

# This function reads current_store_inventory file and returns a dictionary
def get_current_inventory(file_path):
    result_dict = dict()
    with open(file_path, "r") as infile:
        read = csv.reader(infile)
        next(read)
        for row in read:
            if len(row) == 3:
                if row[1] not in result_dict:
                    result_dict[row[1]] = {row[0]:row[2]}
                else:
                    result_dict[row[1]].update({row[0]:row[2]})
    return result_dict

# This function reads store_cluster_map file and returns a dictionary
def get_cluster_info(file_path):
    result_dict = dict()
    with open(file_path, "r") as infile:
        read = csv.reader(infile)
        next(read)
        for row in read:
            if len(row) == 2:
                result_dict[row[0]] = row[1]
    return result_dict

# This function reads projected_probability file and returns a dictionary
def get_item_probability(file_path):
    result_dict = dict()
    with open(file_path, "r") as infile:
        read = csv.reader(infile)
        next(read)
        for row in read:
            if len(row) == 3:
                if row[0] not in result_dict:
                    result_dict[row[0]] = {row[1]:row[2]}
                else:
                    result_dict[row[0]].update({row[1]:row[2]})
    return result_dict

# This function reads bsq file and returns excess, shortage, shortage probability dictionaries
# excess dictionary: Have information about excess item quantity store wise
# shortage dictionary: Have information about shortage item quantity store wise
# shortage probability dictionary: Have information about store probability per item
def process_bsq(file_path, current_inventory_dict, probability_dict):
    excess_dict, shortage_dict, shortage_probability_dict = dict(), dict(), dict()
    with open(file_path, "r") as infile:
        read = csv.reader(infile)
        next(read)
        for row in read:
            if len(row) == 3:
                if row[1] in current_inventory_dict and row[0] in current_inventory_dict[row[1]]:
                    current_quantity = int(current_inventory_dict[row[1]][row[0]])
                    row[2] = int(row[2])
                    if row[2]<current_quantity:
                        if row[1] not in excess_dict:
                            excess_dict[row[1]] = {row[0]:current_quantity-row[2]}
                        else:
                            excess_dict[row[1]].update({row[0]:current_quantity-row[2]})
                    elif row[2]>current_quantity:
                        if row[0] not in shortage_dict:
                            shortage_dict[row[0]] = {row[1]:row[2]-current_quantity}
                            shortage_probability_dict[row[0]] = {row[1]:probability_dict[row[0]][row[1]]}
                        else:
                            shortage_dict[row[0]].update({row[1]:row[2]-current_quantity})
                            shortage_probability_dict[row[0]].update({row[1]:probability_dict[row[0]][row[1]]})
                    else:
                        pass
    return excess_dict, shortage_dict, shortage_probability_dict


# This function prepares order details item wise and returns orders summary and orders itemized dataframes
def get_order_details(excess_dict, shortage_dict, shortage_probability_dict, cluster_dict):
    current_date = datetime.today().strftime('%Y-%m-%d')
    order_item_wise_dict = OrderedDict({'IST Date':[], 'SKU':[], 'From Store':[], 'To Store':[], 'IST Qty':[]})
    for each_excess_store in excess_dict:
        for each_item in excess_dict[each_excess_store]:
            if each_item in shortage_dict:
                sorted_probability_stores = sorted(shortage_probability_dict[each_item].items(), key=lambda kv: kv[1], reverse=True)
                try:
                    for each_store in sorted_probability_stores:
                        each_store = each_store[0]
                        # ISTs can be generated within the stores in the same cluster only
                        if cluster_dict[each_excess_store] != cluster_dict[each_store]:
                            continue
                        if excess_dict[each_excess_store][each_item] == 0:
                            raise Exception()
                        if excess_dict[each_excess_store][each_item] >= shortage_dict[each_item][each_store]:
                            excess_dict[each_excess_store][each_item] = excess_dict[each_excess_store][each_item]-shortage_dict[each_item][each_store]
                            order_item_wise_dict['IST Date'].append(current_date)
                            order_item_wise_dict['SKU'].append(each_item)
                            order_item_wise_dict['From Store'].append(each_excess_store)
                            order_item_wise_dict['To Store'].append(each_store)
                            order_item_wise_dict['IST Qty'].append(shortage_dict[each_item][each_store])
                            del shortage_dict[each_item][each_store]
                            del shortage_probability_dict[each_item][each_store]
                        else:
                            shortage_dict[each_item][each_store] = shortage_dict[each_item][each_store]-excess_dict[each_excess_store][each_item]
                            order_item_wise_dict['IST Date'].append(current_date)
                            order_item_wise_dict['SKU'].append(each_item)
                            order_item_wise_dict['From Store'].append(each_excess_store)
                            order_item_wise_dict['To Store'].append(each_store)
                            order_item_wise_dict['IST Qty'].append(excess_dict[each_excess_store][each_item])
                            excess_dict[each_excess_store][each_item] = 0
                except Exception:
                    continue

    order_item_wise_df = pd.DataFrame(order_item_wise_dict)
    order_summary_df=order_item_wise_df.groupby(['IST Date','From Store','To Store']).size().reset_index(name='Total IST Qty')
    return order_summary_df, order_item_wise_df


# This function is used to add constraints which was described in the contract
def filter_orders(order_summary_df, order_item_wise_df):
    # A transfer will be valid only if it has minimum 5 qtys to be sent from one store to another (i.e. if for a store pair i & j the qty that needs to be shipped from i to j is <5 qty then we can’t send it)
    df1=order_summary_df[order_summary_df['Total IST Qty']>=5]

    # The maximum number of orders that can be generated from a single store is 5 (i.e. a store i can only send the excess products it has to a maximum of 5 other stores within that cluster)
    df2=df1.groupby(['From Store']).filter(lambda x: len(x)<=5)

    # The sum of the qtys across all the orders from a single store should not exceed 50 (i.e. if from store i there are n other stores where we have calculated the transfers to be done then the total qty that is flowing out from store i in all these transfers should be <=50)
    filtered_order_summary_df=df2.loc[df2.groupby(['From Store'])['Total IST Qty'].transform('sum')<=50]

    # Update item wise orders based on order summary
    filtered_order_item_wise_df = filtered_order_summary_df.merge(order_item_wise_df, on=['From Store','To Store', 'IST Date'])
    del filtered_order_item_wise_df['Total IST Qty']
    return filtered_order_summary_df, filtered_order_item_wise_df

# This function generates two csv files as the output
def generate_output(filtered_order_summary_df, filtered_order_item_wise_df):
    if not os.path.exists(output_data_path):
        os.makedirs(output_data_path)
    print("Writing output at %s" % output_data_path)
    filtered_order_summary_df.to_csv(output_data_path+'orders_summary.csv', encoding='utf-8', header='true', index=False)
    filtered_order_item_wise_df.to_csv(output_data_path+'orders_itemized.csv', encoding='utf-8', header='true', index=False)


def main():
    if not os.path.exists(input_data_path):
        raise Exception('Input path not found')
    print("Input path found!")
    print("Started processing orders...")
    current_inventory_dict = get_current_inventory(input_data_path+'current_store_inventory.csv')
    cluster_dict = get_cluster_info(input_data_path+'store_cluster_map.csv')
    probability_dict = get_item_probability(input_data_path+'projected_probability.csv')
    excess_dict, shortage_dict, shortage_probability_dict = process_bsq(input_data_path+'bsq.csv', current_inventory_dict, probability_dict)
    order_summary_df, order_item_wise_df = get_order_details(excess_dict, shortage_dict, shortage_probability_dict, cluster_dict)
    filtered_order_summary_df, filtered_order_item_wise_df = filter_orders(order_summary_df, order_item_wise_df)
    generate_output(filtered_order_summary_df, filtered_order_item_wise_df)



if __name__ == '__main__':
    main()
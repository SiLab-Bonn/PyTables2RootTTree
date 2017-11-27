"""This script converts a hdf5 table into a CERN ROOT Ttree. Tested with ROOT 5.34.36, no support for ROOT 6
"""
import tables as tb
import numpy as np
import ctypes

# from root_numpy import array2root
from ROOT import TFile, TTree
from ROOT import gROOT, AddressOf


def get_root_type_descriptor(numpy_type_descriptor):
    ''' Converts the numpy type descriptor to the ROOT type descriptor.
    Parameters
    ----------
    numpy_type_descriptor: np.dtype
    '''
    return{
        'int64': 'L',
        'uint64': 'l',
        'int32': 'I',
        'uint32': 'i',
        'int16': 'S',
        'uint16': 's',
        'int8': 'B',
        'uint8': 'b',
        'float64' : 'f'
    }[str(numpy_type_descriptor)]


def get_c_type_descriptor(numpy_type_descriptor):
    ''' Converts the numpy type descriptor to the ctype descriptor.
    Parameters
    ----------
    numpy_type_descriptor: np.dtype
    '''
    return{
        'int64': ctypes.c_longlong,
        'uint64': ctypes.c_ulonglong,
        'int32': ctypes.c_int,
        'uint32': ctypes.c_uint,
        'int16': ctypes.c_short,
        'uint16': ctypes.c_ushort,
        'int8': ctypes.c_byte,
        'uint8': ctypes.c_ubyte,
        'float64' : ctypes.c_longdouble,
    }[str(numpy_type_descriptor)]


def init_tree_from_table(table, chunk_size=1):
    ''' Initializes a ROOT tree from a HDF5 table.
    Takes the HDF5 table column names and types and creates corresponding branches. If a chunk size is specified the branches will have the length of the chunk size and
    an additional parameter is returned (as reference) to change the chunk size at a later stage.

    Parameters
    ----------
    table: pytables.table
    chunk_size: int
    '''
    tree_name = table.name # Assign proper name to Tree

    tree = TTree(tree_name, 'Converted HDF5 table')
    n_entries = None
    if chunk_size > 1:
        n_entries = ctypes.c_int(chunk_size)
        tree.Branch('n_entries', ctypes.addressof(n_entries), 'n_entries/I')  # needs to be added, otherwise one cannot access chunk_size_tree

    for column_name in table.dtype.names:
        tree.Branch(column_name, 'NULL', column_name + '[n_entries]/' + get_root_type_descriptor(table.dtype[column_name]) if chunk_size > 1 else column_name + '/' + get_root_type_descriptor(table.dtype[column_name]))

    return tree, n_entries



def convert_table(input_filename, output_filename, chunk_size = 50000):
    ''' Creates a ROOT Tree by looping over chunks of the hdf5 table. Some pointer magic is used to increase the conversion speed.

    Parameters
    ----------
    input_filename: string
        The file name of the hdf5 hit table.

    output_filename: string
        The filename of the created ROOT file

    '''

    with tb.open_file(input_filename , 'r') as in_file_h5:

        
        out_file_root = TFile(output_filename, 'RECREATE')
        for node in in_file_h5.iter_nodes('/','Table'): # loop over all TABLES in input_filename

            table = node
            tree, chunk_size_tree = init_tree_from_table(table, chunk_size)
    
            for index in range(0, table.shape[0], chunk_size):
                
                hits = table.read(start=index, stop=index + chunk_size)

                column_data = {}  # columns have to be in an additional python data container to prevent the carbage collector from deleting
    
                for branch in tree.GetListOfBranches():  # loop over the branches
                    if branch.GetName() != 'n_entries':
                        column_data[branch.GetName()] = np.ascontiguousarray(hits[branch.GetName()])  # a copy has to be made to get the correct memory alignement
                        branch.SetAddress(column_data[branch.GetName()].data)  # get the column data pointer by name and tell the tree its address
    
                if index + chunk_size > table.shape[0]:  # decrease tree leave size for the last chunk
                    chunk_size_tree.value = table.shape[0] - index
    
                tree.Fill()

            out_file_root.Write()
        out_file_root.Close()


if __name__ == "__main__":
    
    input_file = '/media/data/SHiP/SHiP-testbeam-September17/testbeam-analysis/tba_improvements_branch/board_10/set_trigger_delay/module_2/69_module_2_ext_trigger_scan_interpreted'
    
    convert_table(input_file + '.h5', input_file + '.root', chunk_size = 50000)  # chose chunk_size parameter as big as possible to increase speed, but not too big otherwise program runs out of memory

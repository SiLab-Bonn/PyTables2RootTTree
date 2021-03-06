"""
This script converts a HDF5/pytables table into a CERN ROOT TTree.
Tested with ROOT 5.34/38 and ROOT 6.14/08.
"""
import ctypes
import os

import tables as tb

from ROOT import TFile, TTree, TObject


def get_root_type_descriptor(numpy_type_descriptor):
    ''' Converts the numpy type descriptor to the ROOT type descriptor.

    Parameters
    ----------
    numpy_type_descriptor: numpy.dtype
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
        'float64': 'D',
        'float32': 'F',
        'bool': 'O'
    }[str(numpy_type_descriptor)]


def get_c_type_descriptor(numpy_type_descriptor):
    ''' Converts the numpy type descriptor to the ctype descriptor.

    Parameters
    ----------
    numpy_type_descriptor: numpy.dtype
    '''
    return{
        'int64': ctypes.c_int64,
        'uint64': ctypes.c_uint64,
        'int32': ctypes.c_int32,
        'uint32': ctypes.c_uint32,
        'int16': ctypes.c_int16,
        'uint16': ctypes.c_uint16,
        'int8': ctypes.c_int8,
        'uint8': ctypes.c_uint8,
        'float64': ctypes.c_double,
        'float32': ctypes.c_float,
        'bool': ctypes.c_bool
    }[str(numpy_type_descriptor)]


def init_tree_from_table(table):
    ''' Initializes a ROOT TTree from a HDF5/pytables table.
    A branch (TTree:Branch) is created for each column of the table to avoid alignment problems on different platforms.

    Parameters
    ----------
    table : pytables.table
        Pytables table.
    '''
    # Assign proper name for the TTree
    tree = TTree(table.name, table.name)
    # TTree where branches are variable length arrays
    n_entries = ctypes.c_int64(0)
    tree.Branch('n_entries', ctypes.addressof(n_entries), 'n_entries/L')
    # Adding a branch for each column with address 0 (NULL), the address is set later
    for column_name in table.dtype.names:
        tree.Branch(column_name, 0, column_name + '[n_entries]/' + get_root_type_descriptor(table.dtype[column_name]))

    return tree, n_entries


def convert_table(input_filename, output_filename=None, names=None, chunk_size=100000):
    ''' Creates a ROOT Tree by looping over chunks of the HDF5/pytables table.
    Some pointer magic is used to increase the conversion speed.

    Parameters
    ----------
    input_filename : string
        The filename of the HDF5 input file containing the pytables table.
    output_filename : string
        The filename of the ROOT output file.
    names : string or list
        Table name or list of table names that will be converted to a TTree.
    chunk_size : int
        Chunk size of each read and maximum length of each branch.
    '''
    if os.path.splitext(input_filename)[1].strip().lower() != '.h5':
        base_filename = input_filename
        input_filename = input_filename + '.h5'
    else:
        base_filename = os.path.splitext(input_filename)[0]

    if output_filename is None:
        output_filename = base_filename + '.root'
    else:
        if os.path.splitext(output_filename)[1].strip().lower() != '.root':
            output_filename = output_filename + '.root'

    if not isinstance(names, (list, tuple)) and names is not None:
        names = [names]

    with tb.open_file(input_filename, 'r') as in_file_h5:
        out_file_root = TFile(output_filename, 'RECREATE')
        # Loop over all tables in the input file
        for table in in_file_h5.iter_nodes('/', 'Table'):
            if names is not None and table.name not in names:
                continue
            tree, n_entries = init_tree_from_table(table)
            for index in range(0, table.shape[0], chunk_size):
                hits = table.read(start=index, stop=index + chunk_size)
                # Columns have to be in an additional python data container to prevent the carbage collector from deleting
                column_data = {}
                # Loop over the columns
                for column_name in table.dtype.names:
                    branch = tree.GetBranch(column_name)
                    # Get a copy of the column
                    column_data[column_name] = hits[column_name].copy()
                    # Get the column data pointer by name and tell the tree its address
                    branch.SetAddress(column_data[column_name].data)
                # Set array size
                n_entries.value = hits.shape[0]
                # Fill TTree
                tree.Fill()
            # Write ROOT file, delete the previous cycle (written by AutoSave)
            out_file_root.Write("", TObject.kOverwrite)
        # Close ROOT file
        out_file_root.Close()


if __name__ == "__main__":
    input_files = ['input.h5']
    for file in input_files:
        convert_table(file)

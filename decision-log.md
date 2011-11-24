# Class for extracting the information to be stored in the index?

This class would only be useful for shrinking the size of a primary index, so we leaf it out.

# Changing Split Sizes

We currently do not support altering split sizes. This is because we store only the position range for a BTree file
in the properties. This means, that if the IndexedRecordReader split does start and end on exactly this points,
we have no way of telling which parts of the btree are within the position range and which parts are not.

Right now we throw an exception if the RecordReader borders do not match BTree boarders, later we should fix this.

Well, for the SecondaryIndex this can be checked easily!
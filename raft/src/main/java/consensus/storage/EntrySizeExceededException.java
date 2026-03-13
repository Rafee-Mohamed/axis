package consensus.storage;

/**
 * Max allowed entries size exceeded.
 */
class EntrySizeExceededException extends StorageException {
    public EntrySizeExceededException(long exceededSize) {
        super("Max allowed entries size exceeded - " + exceededSize);
    }
}

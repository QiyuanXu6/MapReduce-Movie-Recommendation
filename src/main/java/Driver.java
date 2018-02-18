
public class Driver {
	public static void main(String[] args) throws Exception {

		DataDividerByUser dataDividerByUser = new DataDividerByUser();
		CoOccurrenceMatrixGenerator coOccurrenceMatrixGenerator = new CoOccurrenceMatrixGenerator();
		Normalize normalize = new Normalize();
		Multiplication multiplication = new Multiplication();
		Sum sum = new Sum();

		String rawInput = args[0];
		String userMovieListOutputDir = args[1];
		String coOccurranceMatrixDir = args[2];
		String normalizeDir = args[3];
		String multiplicationDir = args[4];
		String sumDir = args[5];

		// from raw data to user: (mid,rating)
		String[] path1 = {rawInput, userMovieListOutputDir};
		// from user:(mid, rating) to mid1,mid2:count
		String[] path2 = {userMovieListOutputDir, coOccurranceMatrixDir};
		// from mid1,mid2:count/total
		String[] path3 = {coOccurranceMatrixDir, normalizeDir};
		String[] path4 = {normalizeDir, rawInput, multiplicationDir};
		// from
		String[] path5 = {multiplicationDir, sumDir};

		dataDividerByUser.main(path1);
		coOccurrenceMatrixGenerator.main(path2);
		normalize.main(path3);
		multiplication.main(path4);
		sum.main(path5);
	}

}

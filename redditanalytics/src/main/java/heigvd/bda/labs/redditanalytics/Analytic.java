package  heigvd.bda.labs.redditanalytics;

public class Analytic {
	private Submission submission;
	private Comment comment;

	Analytic(String json)
	{
		ConstructPostComment(json);
	}

	Analytic()
	{
		this.submission = new Submission();
		this.comment = new Comment();
	}
	
	public void setJson(String json) {
		ConstructPostComment(json);
	}
	
	private void ConstructPostComment(String json) {
		this.submission = new Submission(json);
		this.comment = new Comment(this.submission.getComment());
	}
	
	public Submission getSubmission(){
		return this.submission;
	}
	
	public Comment getComment(){
		return this.comment;
	}
}
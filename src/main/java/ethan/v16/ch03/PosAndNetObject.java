package ethan.v16.ch03;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.Text;
import org.eclipse.jdt.core.Flags;

/**
 * @since 2014-9-27
 * @author ethan
 */

public class PosAndNetObject {

	enum AnalysisError {
		OUTOFDATE("统计日期不存在", 0), WRONGDATE("日志日期不正确", -1), ANALYERROR("分析错误", -2);
		private int code;
		private String msg;

		AnalysisError(String msg, int code) {
			this.code = code;
			this.msg = msg;
		}

		public int get() {
			return this.code;
		}

		public String msg() {
			return this.msg;
		}
	}

	class AnalysisException extends Exception {

		private static final long serialVersionUID = -276033106880946869L;
		private AnalysisError error;
		public AnalysisException(AnalysisError error) {
			super(error.msg());
			this.error = error;
		}
		
		public AnalysisError getCounter(){
			return this.error;
		}

	}

	// 位置数据
	// IMSI|IMEI|UPDATETYPE|CGI|TIME
	// 上网数据
	// IMSI|IMEI|CGI|TIME|CGI|URL

	// 0000000000 0054775807 3 00000024 2013-09-12 00:57:18
	/**
	 * 基站
	 */
	private String position;
	/**
	 * 国际移动用户识别码
	 */
	String imsi;
	/**
	 * 
	 */
	String logTime;
	String flag;
	private SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	protected Date logDate;

	// ^^消化其他异常，抛出我的异常
	public void set(String line, boolean analysisPos, String analysisHours, String analysisDay) throws AnalysisException {
		String[] arr = line.split("\t");
		if (analysisPos) {
			imsi = arr[0];
			position = arr[3];
			logTime = arr[4];
		} else {
			imsi = arr[0];
			position = arr[2];
			logTime = arr[3];
		}
		// 2个检查
		// 1、日志的日期与分析的日期有出入
		// 2.日志的日期格式不对
		if (!logTime.startsWith(analysisDay))
			throw new AnalysisException(AnalysisError.OUTOFDATE);

		try {
			this.logDate = formatter.parse(logTime);
		} catch (ParseException e) {
			throw new AnalysisException(AnalysisError.WRONGDATE);
		}
		
		//处理统计区间
		String[] flags = analysisHours.split("-");
		int i=0,n= flags.length;
		//日志小时与区间位置i的值Vi比较,当Vi容纳不了日志小时后，i值递增，用i的位置取区间标识
		int hour = Integer.parseInt(logTime.split(" ")[1].split(":")[0]);
//		flags[0] vs hour
//               <   i++  进入区间下一值
		while(i<n && hour>=Integer.valueOf(flags[i])) i++;
		if(i<n){
			if(i==0){
				flag = "00"+"-"+flags[i];
			}else{
				flag = flags[i-1]+"-"+flags[i];
			}
		}else throw new AnalysisException(AnalysisError.ANALYERROR);
	}
	
	public Text keyOut(){
		// key 卡+时间段
		return new Text(this.imsi+"|"+this.flag);
	}
	
	public Text valueOut() {
		// logTime
		long t = this.logDate.getTime() /1000L;
		return new Text(this.position+"|"+ String.valueOf(t));
	}
	
	public Text keyOut2(){
		// key 卡+时间段
		return new Text(this.imsi);
	}
	
	public Text valueOut2() {
		// logTime
		long t = this.logDate.getTime() /1000L;
		return new Text(this.position +"|"+ this.flag+"|"+ String.valueOf(t));
	}

}

package io.mycat.server.config.node;

import java.util.HashMap;
import java.util.Map;

public class SequenceConfig {
	private String type;
	private String vclass;
	private Map<String, Object> props = new HashMap<String, Object>();

	public Map<String, Object> getProps() {
		return props;
	}
	public void setProps(Map<String, Object> props) {
		this.props = props;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getVclass() {
		return vclass;
	}
	public void setVclass(String vclass) {
		this.vclass = vclass;
	}


}

package edu.cornell.library.integration.authority.mads;

import java.util.ArrayList;
import java.util.List;

public class ComponentList {
	public List<Component> components = null;
	public ComponentList() {
		components = new ArrayList<>();
	}

	public void addComponent(String id, HeadingType headingType, String authorativeLabel) {
		components.add(new Component(id, headingType, authorativeLabel));
	}

	public Component first() {
		if (components.isEmpty()) return null;
		return components.get(0);
	}

	public List<Component> restComponents() {
		if (components.size() <= 1) return new ArrayList<>();
		return components.subList(1, components.size());
	}

	public class Component {
		public String id;
		public HeadingType headingType;
		public String authorativeLabel;
		public Component(String id, HeadingType headingType, String authorativeLabel) {
			this.id = id;
			this.headingType = headingType;
			this.authorativeLabel = authorativeLabel;
		}
	}
}

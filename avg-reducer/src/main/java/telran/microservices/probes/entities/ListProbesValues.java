package telran.microservices.probes.entities;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.springframework.data.redis.core.RedisHash;

import lombok.Getter;

@RedisHash
@Getter
public class ListProbesValues {
	long id;
	List<Integer> values = new ArrayList<>();

	public ListProbesValues(long id) {
		super();
		this.id = id;
	}

	@Override
	public int hashCode() {
		return Objects.hash(id);
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!(obj instanceof ListProbesValues))
			return false;
		ListProbesValues other = (ListProbesValues) obj;
		return id == other.id;
	}
	
}

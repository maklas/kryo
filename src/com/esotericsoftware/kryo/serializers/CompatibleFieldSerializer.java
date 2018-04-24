
package com.esotericsoftware.kryo.serializers;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.InputChunked;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.io.OutputChunked;
import com.esotericsoftware.kryo.util.ObjectMap;

/** Serializes objects using direct field assignment, with limited support for forward and backward compatibility. Fields can be
 * added or removed without invalidating previously serialized bytes. Note that changing the type of a field is not supported.
 * <p>
 * There is additional overhead compared to {@link FieldSerializer}. A header is output the first time an object of a given type
 * is serialized. The header consists of an int for the number of fields, then a String for each field name. Also, to support
 * skipping the bytes for a field that no longer exists, for each field value an int is written that is the length of the value in
 * bytes.
 * <p>
 * Note that the field data is identified by name. The situation where a super class has a field with the same name as a subclass
 * must be avoided.
 * @author Nathan Sweet <misc@n4te.com> */
public class CompatibleFieldSerializer<T> extends FieldSerializer<T> {
	public CompatibleFieldSerializer (Kryo kryo, Class type) {
		super(kryo, type);
	}

	public void write (Kryo kryo, Output output, T object) {
		CachedField[] fields = getFields();
		ObjectMap context = kryo.getGraphContext();
		if (!context.containsKey(this)) {
			context.put(this, null);
			output.writeVarInt(fields.length, true);
			for (CachedField field : fields) output.writeString(field.field.getName());
		}

		OutputChunked outputChunked = new OutputChunked(output, 1024);
		for (CachedField field : fields) {
			field.write(outputChunked, object);
			outputChunked.endChunks();
		}
	}

	public T read (Kryo kryo, Input input, Class<T> type) {
		T object = create(kryo, input, type);
		kryo.reference(object);
		ObjectMap context = kryo.getGraphContext();
		CachedField[] fields = (CachedField[])context.get(this);
		if (fields == null) {
			int length = input.readVarInt(true);
			String[] names = new String[length];
			for (int i = 0; i < length; i++)
				names[i] = input.readString();

			fields = new CachedField[length];
			CachedField[] allFields = getFields();
			outer:
			for (int i = 0, n = names.length; i < n; i++) {
				String schemaName = names[i];
				for (CachedField allField : allFields) {
					if (allField.field.getName().equals(schemaName)) {
						fields[i] = allField;
						continue outer;
					}
				}
			}
			context.put(this, fields);
		}

		InputChunked inputChunked = new InputChunked(input, 1024);
		boolean hasGenerics = getGenerics() != null;
		for (CachedField field : fields) {
			CachedField cachedField = field;
			if (cachedField != null && hasGenerics) {
				// Generic type used to instantiate this field could have 
				// been changed in the meantime. Therefore take the most 
				// up-to-date definition of a field
				cachedField = getField(cachedField.field.getName());
			}
			if (cachedField == null) {
				inputChunked.nextChunks();
				continue;
			}
			cachedField.read(inputChunked, object);
			inputChunked.nextChunks();
		}
		return object;
	}
}

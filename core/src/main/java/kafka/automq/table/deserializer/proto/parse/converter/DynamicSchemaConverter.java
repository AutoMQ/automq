package kafka.automq.table.deserializer.proto.parse.converter;

import com.squareup.wire.schema.internal.parser.OptionElement;
import com.squareup.wire.schema.internal.parser.ServiceElement;
import com.squareup.wire.schema.internal.parser.TypeElement;
import java.util.List;

/**
 * Interface defining the contract for converting source objects into components needed for DynamicSchema creation.
 * This interface provides a standardized way to extract necessary information from different source types
 * that can be used to build a DynamicSchema.
 *
 * @param <T> The type of the source object to convert from (e.g., ProtoFileElement, FileDescriptorProto)
 */
public interface DynamicSchemaConverter<T> {
    
    /**
     * Extracts the syntax version from the source.
     *
     * @param source The source object
     * @return The syntax version as a string, or null if not specified
     */
    String getSyntax(T source);

    /**
     * Retrieves the package name from the source.
     *
     * @param source The source object
     * @return The package name, or null if not specified
     */
    String getPackageName(T source);

    /**
     * Gets all type definitions (messages and enums) from the source.
     *
     * @param source The source object
     * @return List of TypeElements representing messages and enums
     */
    List<TypeElement> getTypes(T source);

    /**
     * Retrieves the list of imports from the source.
     *
     * @param source The source object
     * @return List of import file paths
     */
    List<String> getImports(T source);

    /**
     * Retrieves the list of public imports from the source.
     *
     * @param source The source object
     * @return List of public import file paths
     */
    List<String> getPublicImports(T source);

    /**
     * Retrieves all options defined in the source.
     *
     * @param source The source object
     * @return List of OptionElements
     */
    List<OptionElement> getOptions(T source);

    /**
     * Retrieves all service definitions from the source.
     * @param source The source object
     * @return List of ServiceElements
     */
    List<ServiceElement> getServices(T source);

} 
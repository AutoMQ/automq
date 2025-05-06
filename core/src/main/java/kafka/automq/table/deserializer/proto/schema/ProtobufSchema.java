/*
 * Copyright 2021 Red Hat
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.automq.table.deserializer.proto.schema;

import com.google.protobuf.Descriptors.FileDescriptor;
import com.squareup.wire.schema.internal.parser.ProtoFileElement;
import java.util.Objects;

public class ProtobufSchema {

    private final FileDescriptor fileDescriptor;
    private ProtoFileElement protoFileElement;

    public ProtobufSchema(FileDescriptor fileDescriptor, ProtoFileElement protoFileElement) {
        Objects.requireNonNull(fileDescriptor);
        Objects.requireNonNull(protoFileElement);
        this.fileDescriptor = fileDescriptor;
        this.protoFileElement = protoFileElement;
    }

    /**
     * @return the fileDescriptor
     */
    public FileDescriptor getFileDescriptor() {
        return fileDescriptor;
    }

    /**
     * @return the protoFileElement
     */
    public ProtoFileElement getProtoFileElement() {
        return protoFileElement;
    }

}


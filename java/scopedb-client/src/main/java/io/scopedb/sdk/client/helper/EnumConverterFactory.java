/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.scopedb.sdk.client.helper;

import com.google.gson.annotations.SerializedName;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import org.jetbrains.annotations.NotNull;
import retrofit2.Converter;
import retrofit2.Retrofit;

public final class EnumConverterFactory extends Converter.Factory {
    @Override
    public Converter<?, String> stringConverter(
            @NotNull Type type, Annotation @NotNull [] annotations, @NotNull Retrofit retrofit) {
        if (getRawType(type).isEnum()) {
            return new EnumConverter();
        }
        return null;
    }
}

final class EnumConverter implements Converter<Enum<?>, String> {
    @Override
    public String convert(@NotNull Enum<?> o) {
        try {
            final Field f = o.getClass().getField(o.name());
            final SerializedName name = f.getAnnotation(SerializedName.class);
            if (name != null) {
                return name.value();
            }
        } catch (Exception ignored) {
            // passthrough
        }
        return o.name();
    }
}

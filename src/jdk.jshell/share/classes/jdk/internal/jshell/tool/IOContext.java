/*
 * Copyright (c) 2015, 2024, Oracle and/or its affiliates. All rights reserved.
 * DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
 *
 * This code is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License version 2 only, as
 * published by the Free Software Foundation.  Oracle designates this
 * particular file as subject to the "Classpath" exception as provided
 * by Oracle in the LICENSE file that accompanied this code.
 *
 * This code is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General Public License
 * version 2 for more details (a copy is included in the LICENSE file that
 * accompanied this code).
 *
 * You should have received a copy of the GNU General Public License version
 * 2 along with this work; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA.
 *
 * Please contact Oracle, 500 Oracle Parkway, Redwood Shores, CA 94065 USA
 * or visit www.oracle.com if you need additional information or have any
 * questions.
 */

package jdk.internal.jshell.tool;

import java.io.IOException;
import java.io.Writer;
import java.nio.charset.Charset;

/**
 * Interface for defining user interaction with the shell.
 * @author Robert Field
 */
abstract class IOContext implements AutoCloseable {

    @Override
    public abstract void close() throws IOException;

    public abstract String readLine(String firstLinePrompt, String continuationPrompt, boolean firstLine, String prefix) throws IOException, InputInterruptedException;

    public abstract boolean interactiveOutput();

    public abstract Iterable<String> history(boolean currentSession);

    public abstract  boolean terminalEditorRunning();

    public abstract void suspend();

    public abstract void resume();

    public abstract void beforeUserCode();

    public abstract void afterUserCode();

    public abstract void replaceLastHistoryEntry(String source);

    public abstract int readUserInput() throws IOException;

    public int readUserInputChar() throws IOException {
        return -1;
    }

    public String readUserLine(String prompt) throws IOException {
        userOutput().write(prompt);
        userOutput().flush();
        return null;
    }

    public String readUserLine() throws IOException {
        return null;
    }

    public Writer userOutput() {
        throw new UnsupportedOperationException();
    }

    public char[] readPassword(String prompt) throws IOException {
        userOutput().write(prompt);
        userOutput().flush();
        return null;
    }

    public void setIndent(int indent) {}

    public Charset charset() {
        throw new UnsupportedOperationException();
    }

    class InputInterruptedException extends Exception {
        private static final long serialVersionUID = 1L;
    }
}


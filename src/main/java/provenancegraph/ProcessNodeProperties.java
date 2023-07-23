package provenancegraph;

import java.util.Objects;

import static utils.EntityGeneralizer.argumentsGeneralizer;
import static utils.EntityGeneralizer.exeFilePathGeneralizer;

public class ProcessNodeProperties extends NodeProperties {
    private int processId;
    private String exePath;
    private String cmdLineArguments;

    public ProcessNodeProperties(int processId, String exePath, String cmdLineArguments) {
        this.type = NodeType.Process;
        this.processId = processId;
        this.exePath = exePath;
        this.cmdLineArguments = cmdLineArguments;
    }

    @Override
    public String toString() {
        return String.format("[%s: PID-%s, PATH-%s, ARGUMENTS-%s]", this.type.toString(), this.processId, this.exePath, this.cmdLineArguments);
    }

//    @Override
//    public boolean haveSameProperties(NodeProperties np) {
//        ProcessNodeProperties that = (ProcessNodeProperties) np;
//        return exePath.equals(that.exePath) && cmdLineArguments.equals(that.cmdLineArguments);
//    }

    @Override
    public boolean equals(Object o) {
        if (!super.equals(o)) return false;
        else {
            ProcessNodeProperties that = (ProcessNodeProperties) o;
            return exePath.equals(that.exePath) && cmdLineArguments.equals(that.cmdLineArguments);
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), exePath, cmdLineArguments);
    }

    public void generalize() {
        this.exePath = exeFilePathGeneralizer(exePath);
        this.cmdLineArguments = argumentsGeneralizer(cmdLineArguments);
    }

    @Override
    public ProcessNodeProperties copyGeneralize() {
        return new ProcessNodeProperties(
                this.processId,
                exeFilePathGeneralizer(exePath),
                argumentsGeneralizer(cmdLineArguments)
        );
    }
}
